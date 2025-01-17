from __future__ import annotations

import asyncio
import ctypes
import logging
import os
import threading
import time
from collections.abc import Callable  # noqa: TCH003
from dataclasses import dataclass
from typing import Any

from . import bindings
from .lib import tb_assert, c_uint128

logger = logging.getLogger("tigerbeetle")


class AtomicInteger:
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += 1
            return self._value


@dataclass
class CompletionContextSync:
    event: threading.Event


@dataclass
class CompletionContextAsync:
    loop: asyncio.AbstractEventLoop
    event: asyncio.Event


@dataclass
class InflightPacket:
    packet: bindings.CPacket
    response: Any
    operation: bindings.Operation
    c_event_type: Any
    c_result_type: Any
    on_completion: Callable | None
    on_completion_context: CompletionContextSync | CompletionContextAsync | None



def id() -> int:
    """
    Generates a Universally Unique and Sortable Identifier as a 128-bit integer. Based on ULIDs.
    """
    time_ms = time.time_ns() // (1000 * 1000)

    # Ensure time_ms monotonically increases.
    time_ms_last = getattr(id, "_time_ms_last", 0)
    if time_ms <= time_ms_last:
        time_ms = time_ms_last
    else:
        id._time_ms_last = time_ms

    randomness = os.urandom(10)

    return int.from_bytes(
        time_ms.to_bytes(6, "big") + randomness,
        "big",
    )


amount_max = (2 ** 128) - 1


class InitError(Exception):
    pass

class PacketError(Exception):
    pass


class Client:
    _clients: dict[int, Any] = {}
    _counter = AtomicInteger()

    def __init__(self, cluster_id: int, replica_addresses: str):
        self._client_key = Client._counter.increment()
        self._client = bindings.Client()

        self._inflight_packets: dict[int, InflightPacket] = {}

        # ctypes needs a reference to keep this alive through the FFI call. Having it as a temporary
        # within the call _does not_ work.
        cluster_id_u128 = c_uint128.from_param(cluster_id)
        init_status = bindings.tb_client_init(
            ctypes.byref(self._client),
            ctypes.cast(
                ctypes.byref(cluster_id_u128), ctypes.POINTER(ctypes.c_uint8 * 16)
            ),
            replica_addresses.encode("ascii"),
            len(replica_addresses),
            self._client_key,
            self._c_on_completion
        )
        if init_status != bindings.Status.SUCCESS:
            raise InitError(init_status)

        Client._clients[self._client_key] = self


    def _acquire_packet(self, operation: bindings.Operation, operations: Any,
                        c_event_type: Any, c_result_type: Any) -> InflightPacket:
        packet = bindings.CPacket()
        packet.next = None
        packet.user_data = Client._counter.increment()
        packet.operation = operation
        packet.status = bindings.PacketStatus.OK

        operations_array_type = c_event_type * len(operations)
        operations_array = operations_array_type(*map(c_event_type.from_param, operations))

        packet.data_size = ctypes.sizeof(operations_array)
        packet.data = ctypes.cast(operations_array, ctypes.c_void_p)

        return InflightPacket(
            packet=packet,
            response=None,
            on_completion=None,
            on_completion_context=None,
            operation=operation,
            c_event_type=c_event_type,
            c_result_type=c_result_type)

    def close(self):
        bindings.tb_client_deinit(self._client)
        tb_assert(self._client is not None)
        tb_assert(len(self._inflight_packets) == 0)
        del Client._clients[self._client_key]

    @staticmethod
    @bindings.OnCompletion
    def _c_on_completion(completion_ctx, tb_client, packet, timestamp, bytes_ptr, len_):
        """
        Invoked in a separate thread
        """
        self: Client = Client._clients[completion_ctx]
        tb_assert(self._client.value == tb_client)

        packet = ctypes.cast(packet, ctypes.POINTER(bindings.CPacket))
        inflight_packet = self._inflight_packets[packet[0].user_data]

        if packet[0].status != bindings.PacketStatus.OK.value:
            inflight_packet.response = PacketError(repr(bindings.PacketStatus(packet[0].status)))
            tb_assert(inflight_packet.on_completion is not None)
            inflight_packet.on_completion(inflight_packet)
            return

        c_result_type = inflight_packet.c_result_type
        tb_assert(len_ % ctypes.sizeof(c_result_type) == 0)

        # The memory referenced in bytes_ptr is only valid for the duration of this callback. Copy
        # it to a fresh, Python owned buffer and do the conversion from the raw C type to the Python
        # dataclass.
        results_slice = ctypes.cast(
            bytes_ptr,
            ctypes.POINTER(c_result_type)
        )[0:(len_ // ctypes.sizeof(c_result_type))]
        results = [result.to_python() for result in results_slice]

        inflight_packet.response = results

        tb_assert(inflight_packet.on_completion is not None)
        inflight_packet.on_completion(inflight_packet)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ClientSync(Client, bindings.StateMachineMixin):
    def _on_completion(self, inflight_packet):
        inflight_packet.on_completion_context.event.set()

    def _submit(self, operation: bindings.Operation, operations: list[Any],
                c_event_type: Any, c_result_type: Any):
        inflight_packet = self._acquire_packet(operation, operations, c_event_type, c_result_type)
        self._inflight_packets[inflight_packet.packet.user_data] = inflight_packet

        inflight_packet.on_completion = self._on_completion
        inflight_packet.on_completion_context = CompletionContextSync(event=threading.Event())

        bindings.tb_client_submit(self._client, ctypes.byref(inflight_packet.packet))
        inflight_packet.on_completion_context.event.wait()

        del self._inflight_packets[inflight_packet.packet.user_data]

        if isinstance(inflight_packet.response, Exception):
            raise inflight_packet.response

        return inflight_packet.response


class ClientAsync(Client, bindings.AsyncStateMachineMixin):
    def _on_completion(self, inflight_packet):
        """
        Called by Client._c_on_completion, which itself is called from a different thread. Use
        `call_soon_threadsafe` to return to the thread of the event loop the request was invoked
        from, so _trigger_event() can trigger the async event and allow the client to progress.
        """
        inflight_packet.on_completion_context.loop.call_soon_threadsafe(
            self._trigger_event,
            inflight_packet
        )

    def _trigger_event(self, inflight_packet):
        inflight_packet.on_completion_context.event.set()

    async def _submit(self, operation: bindings.Operation, operations: Any,
                      c_event_type: Any, c_result_type: Any):
        inflight_packet = self._acquire_packet(operation, operations, c_event_type, c_result_type)
        self._inflight_packets[inflight_packet.packet.user_data] = inflight_packet

        inflight_packet.on_completion = self._on_completion
        inflight_packet.on_completion_context = CompletionContextAsync(
            loop=asyncio.get_event_loop(),
            event=asyncio.Event()
        )

        bindings.tb_client_submit(self._client, ctypes.byref(inflight_packet.packet))
        await inflight_packet.on_completion_context.event.wait()

        del self._inflight_packets[inflight_packet.packet.user_data]

        if isinstance(inflight_packet.response, Exception):
            raise inflight_packet.response

        return inflight_packet.response


@bindings.LogHandler
def log_handler(level_zig, message_ptr, message_len):
    level_python = {
        0: logging.ERROR,
        1: logging.WARNING,
        2: logging.INFO,
        3: logging.DEBUG,
    }[level_zig]
    logger.log(level_python, ctypes.string_at(message_ptr, message_len).decode("utf-8"))

tb_assert(bindings.tb_client_register_log_callback(log_handler, True) ==
    bindings.RegisterLogCallbackStatus.SUCCESS)

def configure_logging(*, debug, log_handler=log_handler):
    # First disable the existing log handler, before enabling the new one.
    tb_assert(bindings.tb_client_register_log_callback(None, debug) ==
        bindings.RegisterLogCallbackStatus.SUCCESS)

    tb_assert(bindings.tb_client_register_log_callback(log_handler, debug) ==
        bindings.RegisterLogCallbackStatus.SUCCESS)
