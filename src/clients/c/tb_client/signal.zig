const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../tb_client.zig").vsr;
const Time = vsr.time.Time;
const IO = vsr.io.IO;

const Atomic = std.atomic.Value;

/// A Signal is a way to trigger a registered callback on a tigerbeetle IO instance
/// when notification occurs from another thread.
pub const Signal = struct {
    io: *IO,
    event: IO.Event,
    listening: Atomic(bool),
    completion: IO.Completion,

    on_signal_fn: *const fn (*Signal) void,
    state: Atomic(enum(u8) {
        running,
        waiting,
        notified,
        shutdown,
    }),

    pub fn init(self: *Signal, io: *IO, on_signal_fn: *const fn (*Signal) void) !void {
        const event = try io.open_event();
        errdefer io.close_event(event);

        self.* = .{
            .io = io,
            .event = event,
            .listening = Atomic(bool).init(true),
            .completion = undefined,
            .on_signal_fn = on_signal_fn,
            .state = @TypeOf(self.state).init(.running),
        };

        self.wait();
    }

    pub fn deinit(self: *Signal) void {
        assert(self.event != IO.INVALID_EVENT);
        assert(self.stop_requested());

        self.io.close_event(self.event);
        self.* = undefined;
    }

    /// Stops the current event.
    /// Further calls to "notify" will not fire the signal.
    /// Safe to call from multiple threads.
    pub fn stop(self: *Signal) void {
        const listening = self.listening.swap(false, .release);
        if (listening) {
            self.notify();
        }
    }

    pub fn stop_requested(self: *const Signal) bool {
        return !self.listening.load(.acquire);
    }

    /// Schedules the on_signal callback to be invoked on the IO thread.
    /// Safe to call from multiple threads.
    pub fn notify(self: *Signal) void {
        // Try to transition from `waiting` to `notified`.
        // If it fails, analyze the current state to determine if a notification is needed.
        var state: @TypeOf(self.state.raw) = .waiting;
        while (self.state.cmpxchgStrong(
            state,
            .notified,
            .release,
            .acquire,
        )) |state_actual| {
            switch (state_actual) {
                .waiting, .running => state = state_actual, // Try again.
                .notified => return, // Already notified.
                .shutdown => return, // Ignore notifications after shutdown.
            }
        }

        if (state == .waiting) {
            self.io.event_trigger(self.event, &self.completion);
        }
    }

    fn wait(self: *Signal) void {
        assert(!self.stop_requested());

        const state = self.state.swap(.waiting, .acquire);
        self.io.event_listen(self.event, &self.completion, on_event);
        switch (state) {
            // We should be the only ones who could've started waiting.
            .waiting => unreachable,
            // Wait for a `notify`.
            .running => {},
            // A `notify` was already called in the meantime,
            // calling it again asynchronously.
            .notified => self.notify(),
            // Cannot be called after shutdown.
            .shutdown => unreachable,
        }
    }

    fn on_event(completion: *IO.Completion) void {
        const self: *Signal = @fieldParentPtr("completion", completion);
        const stopped = self.stop_requested();
        const state = self.state.cmpxchgStrong(
            .notified,
            if (stopped) .shutdown else .running,
            .release,
            .acquire,
        ) orelse {
            if (stopped) return;

            (self.on_signal_fn)(self);
            self.wait();
            return;
        };

        switch (state) {
            .running => unreachable, // Multiple racing calls to on_signal().
            .waiting => unreachable, // on_signal() called without transitioning to a waking state.
            .notified => unreachable, // Not possible due to CAS semantics.
            .shutdown => unreachable, // Shutdown is a final state.
        }
    }
};

test "signal" {
    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,
        main_thread_id: std.Thread.Id,
        signal: Signal,

        const delay = 5 * std.time.ns_per_ms;
        const events_count = 5;

        fn run_test() !void {
            var self: Context = .{
                .io = try IO.init(32, 0),
                .main_thread_id = std.Thread.getCurrentId(),
                .signal = undefined,
            };
            defer self.io.deinit();

            try Signal.init(&self.signal, &self.io, on_signal);
            defer self.signal.deinit();

            var timer = Time{};
            const start = timer.monotonic();

            const thread = try std.Thread.spawn(.{}, Context.notify, .{&self});

            // Wait for the number of events to complete.
            while (self.count < events_count) try self.io.tick();

            // Begin shutdown and keep ticking until it's completed.
            self.signal.stop();
            thread.join();

            // Notify after shutdown should be ignored.
            self.signal.notify();

            // Make sure the event was triggered multiple times.
            assert(self.count == events_count);

            // Make sure at least some time has passed.
            const elapsed = timer.monotonic() - start;
            assert(elapsed >= delay);
        }

        fn notify(self: *Context) void {
            assert(std.Thread.getCurrentId() != self.main_thread_id);
            while (!self.signal.stop_requested()) {
                std.time.sleep(delay + 1);

                // Triggering the event:
                self.signal.notify();

                // The same signal may be triggered multiple times,
                // but it should only fire once.
                self.signal.notify();
            }
        }

        fn on_signal(signal: *Signal) void {
            const self: *Context = @fieldParentPtr("signal", signal);
            assert(std.Thread.getCurrentId() == self.main_thread_id);
            assert(!self.signal.stop_requested());
            assert(self.count < events_count);

            self.count += 1;
        }
    }.run_test();
}
