const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;
const log = std.log.scoped(.forest);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const schema = @import("schema.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ScanBufferPool = @import("scan_buffer.zig").ScanBufferPool;
const CompactionInterfaceType = @import("compaction.zig").CompactionInterfaceType;
const CompactionBlocks = @import("compaction.zig").CompactionBlocks;
const BlipStage = @import("compaction.zig").BlipStage;

const table_count_max = @import("tree.zig").table_count_max;

pub fn ForestType(comptime _Storage: type, comptime groove_cfg: anytype) type {
    var groove_fields: []const std.builtin.Type.StructField = &.{};
    var groove_options_fields: []const std.builtin.Type.StructField = &.{};

    for (std.meta.fields(@TypeOf(groove_cfg))) |field| {
        const Groove = @field(groove_cfg, field.name);
        groove_fields = groove_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };

        groove_options_fields = groove_options_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };
    }

    const _Grooves = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = groove_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const _GroovesOptions = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = groove_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    {
        // Verify that every tree id is unique.
        comptime var ids: []const u16 = &.{};

        inline for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            for (std.meta.fields(@TypeOf(Groove.config.ids))) |field| {
                const id = @field(Groove.config.ids, field.name);
                assert(id > 0);
                assert(std.mem.indexOfScalar(u16, ids, id) == null);

                ids = ids ++ [_]u16{id};
            }
        }
    }

    const TreeInfo = struct {
        Tree: type,
        tree_name: []const u8,
        tree_id: u16,
        groove_name: []const u8,
        groove_tree: union(enum) { objects, ids, indexes: []const u8 },
    };

    // Invariants:
    // - tree_infos[tree_id - tree_id_range.min].tree_id == tree_id
    // - tree_infos.len == tree_id_range.max - tree_id_range.min
    const _tree_infos: []const TreeInfo = tree_infos: {
        var tree_infos: []const TreeInfo = &[_]TreeInfo{};
        for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            tree_infos = tree_infos ++ &[_]TreeInfo{.{
                .Tree = Groove.ObjectTree,
                .tree_name = groove_field.name,
                .tree_id = @field(Groove.config.ids, "timestamp"),
                .groove_name = groove_field.name,
                .groove_tree = .objects,
            }};

            if (Groove.IdTree != void) {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = Groove.IdTree,
                    .tree_name = groove_field.name ++ ".id",
                    .tree_id = @field(Groove.config.ids, "id"),
                    .groove_name = groove_field.name,
                    .groove_tree = .ids,
                }};
            }

            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = tree_field.type,
                    .tree_name = groove_field.name ++ "." ++ tree_field.name,
                    .tree_id = @field(Groove.config.ids, tree_field.name),
                    .groove_name = groove_field.name,
                    .groove_tree = .{ .indexes = tree_field.name },
                }};
            }
        }

        var tree_id_min = std.math.maxInt(u16);
        for (tree_infos) |tree_info| tree_id_min = @min(tree_id_min, tree_info.tree_id);

        var tree_infos_sorted: [tree_infos.len]TreeInfo = undefined;
        var tree_infos_set = std.StaticBitSet(tree_infos.len).initEmpty();
        for (tree_infos) |tree_info| {
            const tree_index = tree_info.tree_id - tree_id_min;
            assert(!tree_infos_set.isSet(tree_index));

            tree_infos_sorted[tree_index] = tree_info;
            tree_infos_set.set(tree_index);
        }

        // There are no gaps in the tree ids.
        assert(tree_infos_set.count() == tree_infos.len);

        break :tree_infos tree_infos_sorted[0..];
    };

    const Grid = GridType(_Storage);

    // TODO: With all this trouble, why not just store the compaction memory here and move it out of Tree entirely...
    const CompactionInterface = CompactionInterfaceType(Grid, _tree_infos);

    return struct {
        const Forest = @This();

        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Forest) void;
        const GroovesBitSet = std.StaticBitSet(std.meta.fields(Grooves).len);

        pub const Storage = _Storage;
        pub const groove_config = groove_cfg;
        pub const Grooves = _Grooves;
        pub const GroovesOptions = _GroovesOptions;
        pub const tree_infos = _tree_infos;
        pub const tree_id_range = .{
            .min = tree_infos[0].tree_id,
            .max = tree_infos[tree_infos.len - 1].tree_id,
        };

        const CompactionPipeline = struct {
            const PipelineSlot = struct {
                interface: CompactionInterface,
                pipeline: *CompactionPipeline,
                active_operation: BlipStage,
                compaction_index: usize,
            };

            const compaction_count = (_tree_infos[_tree_infos.len - 1].tree_id - _tree_infos[0].tree_id) * constants.lsm_levels;
            const CompactionBitset = std.StaticBitSet(compaction_count);

            compactions: stdx.BoundedArray(CompactionInterface, compaction_count) = .{},
            bar_active_compactions: CompactionBitset = CompactionBitset.initEmpty(),
            beat_active_compactions: CompactionBitset = CompactionBitset.initEmpty(),
            beat_reserved_compactions: CompactionBitset = CompactionBitset.initEmpty(),

            beat_exhausted: bool = false,

            slots: [3]?PipelineSlot = .{ null, null, null },
            slot_filled_count: usize = 0,
            slot_running_count: usize = 0,

            // FIXME: So technically if we want to be able to use our blocks freely, we need as many reads and writes :/
            // We could do a union, just not sure if it's worth it yet.
            compaction_blocks: []BlockPtr,
            compaction_reads: []Grid.FatRead,
            compaction_writes: []Grid.FatWrite,

            compaction_blocks_split: CompactionBlocks = undefined,

            state: enum { filling, full } = .filling,

            next_tick: Grid.NextTick = undefined,
            grid: *Grid,

            forest: ?*Forest = null,
            callback: ?Callback = null,

            pub fn init(allocator: mem.Allocator, grid: *Grid) !CompactionPipeline {
                const compaction_blocks = try allocator.alloc(BlockPtr, 1024);
                errdefer allocator.free(compaction_blocks);

                for (compaction_blocks, 0..) |*cache_block, i| {
                    errdefer for (compaction_blocks[0..i]) |block| allocator.free(block);
                    cache_block.* = try allocate_block(allocator);
                }
                errdefer for (compaction_blocks) |block| allocator.free(block);

                const compaction_reads = try allocator.alloc(Grid.FatRead, 1024);
                errdefer allocator.free(compaction_reads);

                const compaction_writes = try allocator.alloc(Grid.FatWrite, 1024);
                errdefer allocator.free(compaction_writes);

                return .{
                    .compaction_blocks = compaction_blocks,
                    .compaction_reads = compaction_reads,
                    .compaction_writes = compaction_writes,
                    .grid = grid,
                };
            }

            pub fn deinit(self: *CompactionPipeline, allocator: mem.Allocator) void {
                allocator.free(self.compaction_writes);
                allocator.free(self.compaction_reads);
                for (self.compaction_blocks) |block| allocator.free(block);
                allocator.free(self.compaction_blocks);
            }

            // FIXME: Currently the split by a / b is equal, but it shouldn't be for max performance.
            /// Our input and output blocks (excluding index blocks for now) are split two ways. First, equally by pipeline stage
            /// then by table a / table b:
            /// -------------------------------------------------------------
            /// | Pipeline 0                  | Pipeline 1                  |
            /// |-----------------------------|-----------------------------|
            /// | Table A     | Table B       | Table A     | Table B       |
            /// -------------------------------------------------------------
            fn divide_blocks(self: *CompactionPipeline) CompactionBlocks {
                var minimum_block_count: u64 = 0;
                // We need a minimum of 2 input data blocks; one from each table.
                minimum_block_count += 2;

                // We need a minimum of 1 output data block.
                minimum_block_count += 1;

                // Because we're a 3 stage pipeline, with the middle stage (cpu) having a data dependency on both
                // read and write data blocks, we need to split our memory in the middle. This results in a doubling of
                // what we have so far.
                // FIXME: perhaps read_data_block?? Hmmm input is clearer I think.
                minimum_block_count *= 2;

                // Lastly, reserve our input index blocks. For now, just require we have enough for all tables and pretend like
                // pipelining this isn't a thing.
                // FIXME: This shouldn't do that. The minimum is 2; but we don't really need to hold on to index blocks
                // if we read the data blocks directly. TBC.
                minimum_block_count += 9;

                // FIXME: We can also calculate the optimum number of blocks. Old comment below:
                // // Actually... All the comments below would be to do _all_ the work required. The max amount to do the work required
                // // per beat would then be divided by beat. _however_ the minimums won't change!
                // // Minimum of 2, max lsm_growth_factor+1. Replaces index_block_a and index_block_b too.
                // // Output index blocks are explicitly handled in bar_setup_budget.
                // source_index_blocks: []BlockPtr,
                // // Minimum of 2, one from each table. Max would be max possible data blocks in lsm_growth_factor+1 tables.
                // source_value_blocks: []BlockPtr,
                // // Minimum of 1, max would be max possible data blocks in lsm_growth_factor+1 tables.
                // target_value_blocks: []BlockPtr,

                const blocks = self.compaction_blocks;

                assert(blocks.len >= minimum_block_count);

                const source_index_blocks = blocks[0..9];

                const source_value_pipeline_0_level_a = blocks[10..][0..10];
                const source_value_pipeline_0_level_b = blocks[20..][0..10];
                const source_value_pipeline_1_level_a = blocks[30..][0..10];
                const source_value_pipeline_1_level_b = blocks[40..][0..10];

                const output_value_pipeline_0 = blocks[100..][0..300];
                const output_value_pipeline_1 = blocks[500..][0..300];

                const source_value_blocks = .{ .{ source_value_pipeline_0_level_a, source_value_pipeline_0_level_b }, .{ source_value_pipeline_1_level_a, source_value_pipeline_1_level_b } };
                const target_value_blocks = .{ output_value_pipeline_0, output_value_pipeline_1 };
                const source_value_blocks_max = .{ .{ source_value_pipeline_0_level_a.len, source_value_pipeline_0_level_b.len }, .{ source_value_pipeline_1_level_a.len, source_value_pipeline_1_level_b.len } };
                const target_value_blocks_max = .{ output_value_pipeline_0.len, output_value_pipeline_1.len };

                return .{
                    .source_index_blocks = source_index_blocks,
                    .source_value_blocks = source_value_blocks,
                    .target_value_blocks = target_value_blocks,

                    .source_value_blocks_max = source_value_blocks_max,
                    .target_value_blocks_max = target_value_blocks_max,
                };
            }

            pub fn beat(self: *CompactionPipeline, forest: *Forest, op: u64, callback: Callback) void {
                const compaction_beat = op % constants.lsm_batch_multiple;

                const first_beat = compaction_beat == 0;

                self.slot_filled_count = 0;
                self.slot_running_count = 0;

                if (first_beat) {
                    self.bar_active_compactions = CompactionBitset.initEmpty();

                    for (self.compactions.slice(), 0..) |*compaction, i| {
                        // A compaction is marked as live at the start of a bar.
                        self.bar_active_compactions.set(i);

                        // FIXME: These blocks need to be disjoint. Any way we can assert that?
                        const blocks = .{ self.compaction_blocks[900 + i .. 900 + i + 1], self.compaction_blocks[1000 + i .. 1000 + i + 1] };
                        compaction.bar_setup_budget(constants.lsm_batch_multiple, blocks);
                    }

                    // Split up our internal block pool as needed for the compaction pipelines.
                    self.compaction_blocks_split = self.divide_blocks();
                }

                // At the start of a beat, the active compactions are those that are still active in the bar.
                self.beat_active_compactions = self.bar_active_compactions;

                // FIXME: Assert no compactions are running, and the pipeline is empty in a better way
                // MAybe move to a union enum for state.
                for (self.slots) |slot| assert(slot == null);
                assert(self.callback == null);
                assert(self.forest == null);

                for (self.compactions.slice(), 0..) |*compaction, i| {
                    if (!self.bar_active_compactions.isSet(i)) continue;

                    // Set up the beat depending on what buffers we have available.
                    self.beat_reserved_compactions.set(i);
                    compaction.beat_grid_reserve();
                }

                self.callback = callback;
                self.forest = forest;

                if (forest.compaction_pipeline.compactions.count() == 0) {
                    // FIXME: Do we want to handle the case where we have 0 remaining compactions here too, or in advance_pipeline?
                    // No compactions - we're done! Likely we're < lsm_batch_multiple but it could
                    // be that empty ops were pulsed through.
                    maybe(op < constants.lsm_batch_multiple);

                    self.grid.on_next_tick(beat_finished_next_tick, &self.next_tick);
                    return;
                }

                // Everything up to this point has been sync and deterministic. We now enter
                // async-land!
                // Kick off the pipeline by starting a read. The blip_callback will do the rest,
                // including filling and draining.
                log.info("Firing up pipeline.", .{});
                log.info("============================================================", .{});
                self.state = .filling;
                self.advance_pipeline();
            }

            fn beat_finished_next_tick(next_tick: *Grid.NextTick) void {
                const self = @fieldParentPtr(CompactionPipeline, "next_tick", next_tick);

                assert(self.beat_active_compactions.count() == 0);
                assert(self.slot_filled_count == 0);
                assert(self.slot_running_count == 0);
                for (self.slots) |slot| assert(slot == null);

                assert(self.callback != null and self.forest != null);

                const callback = self.callback.?;
                const forest = self.forest.?;

                self.callback = null;
                self.forest = null;

                callback(forest);
            }

            // FIXME: See comments in the interface.. But this sucks, can be messed up easily, and there must be a better way!
            // Maybe a context struct or something?
            fn blip_callback(compaction_interface_opaque: *anyopaque, maybe_beat_exhausted: ?bool, maybe_bar_exhausted: ?bool) void {
                const compaction_interface: *CompactionInterface = @ptrCast(@alignCast(compaction_interface_opaque));
                const pipeline = @fieldParentPtr(PipelineSlot, "interface", compaction_interface).pipeline;

                const slot = blk: for (0..3) |i| {
                    if (pipeline.slots[i]) |*slot| {
                        if (&slot.interface == compaction_interface) {
                            break :blk slot;
                        }
                    }
                } else @panic("no matching slot");

                // Currently only CPU is allowed to tell us we're exhausted.
                assert(maybe_beat_exhausted == null or slot.active_operation == .merge);
                assert(maybe_bar_exhausted == null or slot.active_operation == .merge);

                if (maybe_beat_exhausted) |beat_exhausted| {
                    log.info("blip_callback: marking beat_active_compactions({}) to be unset...", .{slot.compaction_index});
                    pipeline.beat_exhausted = beat_exhausted;
                }

                // FIXME: Not great doing this here.
                if (maybe_bar_exhausted) |bar_exhausted| {
                    if (bar_exhausted) {
                        // If the bar is exhausted the beat must be exhausted too.
                        assert(maybe_beat_exhausted.?);
                        log.info("blip_callback: unsetting bar_active_compactions({})...", .{slot.compaction_index});
                        pipeline.bar_active_compactions.unset(slot.compaction_index);
                    }
                }

                // TODO: For now, we have a barrier on all stages... We might want to drop this, or
                // have a more advanced barrier based on memory only.
                pipeline.slot_running_count -= 1;

                // We wait for all slots to be finished to keep our alignment.
                if (pipeline.slot_running_count > 0) {
                    return;
                }

                log.info("----------------------------------------------------", .{});
                log.info("blip_callback: all slots joined - advancing pipeline", .{});
                pipeline.advance_pipeline();
            }

            fn advance_pipeline(self: *CompactionPipeline) void {
                const active_compaction_index = self.beat_active_compactions.findFirstSet() orelse {
                    log.info("advance_pipeline: All compactions finished! next_tick'ing handler.", .{});
                    self.grid.on_next_tick(beat_finished_next_tick, &self.next_tick);
                    return;
                };
                log.info("advance_pipeline: Active compaction is: {}", .{active_compaction_index});

                if (self.slots[0]) |*first_slot| {
                    first_slot.interface.fixup_buffers();
                }

                // Advanced the current stages, making sure to start our reads and writes before CPU
                var cpu: ?usize = null;
                for (self.slots[0..self.slot_filled_count], 0..) |*slot_wrapped, i| {
                    const slot: *PipelineSlot = &slot_wrapped.*.?;

                    switch (slot.active_operation) {
                        .read => {
                            assert(cpu == null);

                            if (self.beat_exhausted) {
                                // If we hit beat_exhausted, it means that we need to discard the
                                // results of this read by rolling back the internal state.
                                log.info("!!!!!!!!!! doing undo_blip_read()", .{});
                                slot.interface.undo_blip_read();
                            } else {
                                // Only start CPU work after read if we're not exhausted.
                                cpu = i;
                            }
                        },
                        .merge => {
                            slot.active_operation = .write;
                            self.slot_running_count += 1;
                            log.info("Slot {}, Compaction {}: Calling blip_write", .{ i, active_compaction_index });
                            slot.interface.blip_write(blip_callback);
                        },
                        .write => {
                            if (self.beat_exhausted) {
                                if (self.slot_running_count > 0) {
                                    return;
                                }

                                // If we're in the input exhausted state, we have no more reads that can be done, so don't schedule any more
                                log.info("... ... blip_callback: write done on exhausted beat. Moving to next compaction in sequence.", .{});

                                // FIXME: Resetting these like this sucks.
                                self.beat_active_compactions.unset(slot.compaction_index);

                                self.beat_exhausted = false;
                                self.slot_filled_count = 0;
                                assert(self.slot_running_count == 0);
                                self.state = .filling;
                                self.slots = .{ null, null, null };

                                return self.advance_pipeline();
                            }
                            log.info("... ... blip_callback: write done, starting read on {}.", .{i});
                            slot.active_operation = .read;
                            self.slot_running_count += 1;
                            log.info("Slot {}, Compaction {}: Calling blip_read", .{ i, active_compaction_index });
                            slot.interface.blip_read(blip_callback);
                        },
                    }
                }

                // Fill any empty slots (slots always start in read).
                if (self.state == .filling and !self.beat_exhausted) {
                    const slot_idx = self.slot_filled_count;

                    log.info("Starting slot in: {}...", .{slot_idx});
                    assert(self.slots[slot_idx] == null);

                    self.slots[slot_idx] = .{
                        .pipeline = self,
                        .interface = self.compactions.slice()[active_compaction_index],
                        .active_operation = .read,
                        .compaction_index = active_compaction_index,
                    };

                    // FIXME: Assert beat_blocks_assign is only called once in compaction?
                    if (slot_idx == 0) {
                        self.slots[slot_idx].?.interface.beat_blocks_assign(
                            self.compaction_blocks_split,
                            self.compaction_reads,
                            self.compaction_writes,
                        );
                    }

                    // We always start with a read.
                    log.info("Slot {}, Compaction {}: Calling blip_read", .{ slot_idx, self.slots[slot_idx].?.compaction_index });
                    self.slot_running_count += 1;
                    self.slots[slot_idx].?.interface.blip_read(blip_callback);

                    self.slot_filled_count += 1;

                    if (self.slot_filled_count == 3) {
                        self.state = .full;
                    }
                } else if (self.state == .filling and self.beat_exhausted) {
                    log.info("Pipeline has {} filled slots and is in .filling - but beat exhausted so not filling.", .{self.slot_filled_count});
                }

                // Only then start CPU work.
                if (cpu) |c| {
                    const slot = &self.slots[c].?;

                    slot.active_operation = .merge;
                    self.slot_running_count += 1;
                    log.info("Slot: {} Calling blip_merge", .{c});
                    slot.interface.blip_merge(blip_callback);
                }
            }

            fn beat_end(self: *CompactionPipeline) void {
                var i = self.compactions.count();
                while (i > 0) {
                    i -= 1;

                    // We need to run this for all compactions that ran acquire - even if they transitioned to being finished,
                    // so we can't just use bar_active_compactions.
                    if (!self.beat_reserved_compactions.isSet(i)) continue;

                    // CompactionInterface internally stores a pointer to the real Compaction
                    // interface, so by-value should be OK, but we're a bit all over the place.
                    self.compactions.slice()[i].beat_grid_forfeit();
                    self.beat_reserved_compactions.unset(i);
                }

                assert(self.beat_reserved_compactions.count() == 0);
            }
        };

        progress: ?union(enum) {
            open: struct { callback: Callback },
            checkpoint: struct { callback: Callback },
            compact: struct {
                op: u64,
                callback: Callback,
            },
        } = null,

        compaction_progress: enum { idle, trees_or_manifest, trees_and_manifest } = .idle,

        grid: *Grid,
        grooves: Grooves,
        node_pool: *NodePool,
        manifest_log: ManifestLog,
        manifest_log_progress: enum { idle, compacting, done, skip } = .idle,

        compaction_pipeline: CompactionPipeline,

        scan_buffer_pool: ScanBufferPool,

        pub fn init(
            allocator: mem.Allocator,
            grid: *Grid,
            node_count: u32,
            // (e.g.) .{ .transfers = .{ .cache_entries_max = 128, … }, .accounts = … }
            grooves_options: GroovesOptions,
        ) !Forest {
            // NodePool must be allocated to pass in a stable address for the Grooves.
            const node_pool = try allocator.create(NodePool);
            errdefer allocator.destroy(node_pool);

            // TODO: look into using lsm_table_size_max for the node_count.
            node_pool.* = try NodePool.init(allocator, node_count);
            errdefer node_pool.deinit(allocator);

            var manifest_log = try ManifestLog.init(allocator, grid, .{
                .tree_id_min = tree_id_range.min,
                .tree_id_max = tree_id_range.max,
                // TODO Make this a runtime argument (from the CLI, derived from storage-size-max if
                // possible).
                .forest_table_count_max = table_count_max,
            });
            errdefer manifest_log.deinit(allocator);

            var grooves: Grooves = undefined;
            var grooves_initialized: usize = 0;

            errdefer inline for (std.meta.fields(Grooves), 0..) |field, field_index| {
                if (grooves_initialized >= field_index + 1) {
                    @field(grooves, field.name).deinit(allocator);
                }
            };

            inline for (std.meta.fields(Grooves)) |groove_field| {
                const groove = &@field(grooves, groove_field.name);
                const Groove = @TypeOf(groove.*);
                const groove_options: Groove.Options = @field(grooves_options, groove_field.name);

                groove.* = try Groove.init(allocator, node_pool, grid, groove_options);
                grooves_initialized += 1;
            }

            var compaction_pipeline = try CompactionPipeline.init(allocator, grid);
            errdefer compaction_pipeline.deinit(allocator);

            const scan_buffer_pool = try ScanBufferPool.init(allocator);
            errdefer scan_buffer_pool.deinit(allocator);

            return Forest{
                .grid = grid,
                .grooves = grooves,
                .node_pool = node_pool,
                .manifest_log = manifest_log,

                .compaction_pipeline = compaction_pipeline,

                .scan_buffer_pool = scan_buffer_pool,
            };
        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).deinit(allocator);
            }

            forest.manifest_log.deinit(allocator);
            forest.node_pool.deinit(allocator);
            allocator.destroy(forest.node_pool);

            forest.scan_buffer_pool.deinit(allocator);
        }

        pub fn reset(forest: *Forest) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).reset();
            }

            forest.manifest_log.reset();
            forest.node_pool.reset();
            forest.scan_buffer_pool.reset();

            forest.* = .{
                // Don't reset the grid – replica is responsible for grid cancellation.
                .grid = forest.grid,
                .grooves = forest.grooves,
                .node_pool = forest.node_pool,
                .manifest_log = forest.manifest_log,

                // FIXME: Reset this
                .compaction_pipeline = forest.compaction_pipeline,

                .scan_buffer_pool = forest.scan_buffer_pool,
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            assert(forest.manifest_log_progress == .idle);

            forest.progress = .{ .open = .{ .callback = callback } };

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_commence(&forest.manifest_log);
            }

            forest.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            table: *const schema.ManifestNode.TableInfo,
        ) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(forest.manifest_log_progress == .idle);
            assert(table.label.level < constants.lsm_levels);
            assert(table.label.event != .remove);

            switch (table.tree_id) {
                inline tree_id_range.min...tree_id_range.max => |tree_id| {
                    var tree: *TreeForIdType(tree_id) = forest.tree_for_id(tree_id);
                    tree.open_table(table);
                },
                else => {
                    log.err("manifest_log_open_event: unknown table in manifest: {}", .{table});
                    @panic("Forest.manifest_log_open_event: unknown table in manifest");
                },
            }
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(forest.manifest_log_progress == .idle);
            forest.verify_tables_recovered();

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_complete();
            }
            forest.verify_table_extents();

            const callback = forest.progress.?.open.callback;
            forest.progress = null;
            callback(forest);
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            const compaction_beat = op % constants.lsm_batch_multiple;

            // Note: The first beat of the first bar is a special case. It has op 1, and so
            // no bar_setup is called. bar_finish compensates for this internally currently.
            const first_beat = compaction_beat == 0;
            const last_beat = compaction_beat == constants.lsm_batch_multiple - 1;

            // Setup loop, runs only on the first beat of every bar, before any async work is done.
            if (first_beat) {
                log.info("===============================================================", .{});
                log.info("Bar setup:", .{});
                log.info("===============================================================", .{});
                assert(forest.compaction_pipeline.compactions.count() == 0);

                inline for (0..constants.lsm_levels) |level_b| {
                    inline for (tree_id_range.min..tree_id_range.max) |tree_id| {
                        var tree = tree_for_id(forest, tree_id);

                        // FIXME: Big hack to limit to a single tree for debugging!
                        if (tree_id != 2) {
                            tree.table_immutable.mutability.immutable.flushed = true;
                        } else {
                            assert(tree.compactions.len == constants.lsm_levels);

                            var compaction = &tree.compactions[level_b];

                            // This will return how many compactions and stuff this level needs to do...
                            if (compaction.bar_setup(tree, op)) |info| {
                                // FIXME: Assert len?
                                forest.compaction_pipeline.compactions.append_assume_capacity(CompactionInterface.init(info, compaction));
                                log.info("Target Level: {}, Tree: {s}@{}: {}", .{ level_b, tree.config.name, op, info });
                            }
                        }
                    }
                }
                log.info("===============================================================", .{});
            }

            forest.progress = .{ .compact = .{
                .op = op,
                .callback = callback,
            } };

            // Compaction only starts > lsm_batch_multiple because nothing compacts in the first bar.
            assert(op >= constants.lsm_batch_multiple or forest.compaction_pipeline.compactions.count() == 0);
            assert(forest.compaction_progress == .idle);

            forest.compaction_progress = .trees_or_manifest;
            forest.compaction_pipeline.beat(forest, op, compact_callback);

            // Manifest log compaction. Run on the last beat of the bar.
            // TODO: Figure out a plan wrt the pacing here. Putting it on the last beat kinda-sorta
            // balances out, because we expect to naturally do less other compaction work on the
            // last beat.
            // The first bar has no manifest compaction.
            if (last_beat and op > constants.lsm_batch_multiple) {
                forest.manifest_log_progress = .compacting;
                forest.manifest_log.compact(compact_manifest_log_callback, op);
                forest.compaction_progress = .trees_and_manifest;
            } else {
                assert(forest.manifest_log_progress == .idle);
            }
        }

        fn compact_callback(forest: *Forest) void {
            assert(forest.progress.? == .compact);
            assert(forest.compaction_progress != .idle);

            if (forest.compaction_progress == .trees_and_manifest)
                assert(forest.manifest_log_progress != .idle);

            forest.compaction_progress = if (forest.compaction_progress == .trees_and_manifest) .trees_or_manifest else .idle;

            if (forest.compaction_progress != .idle) {
                return;
            }

            forest.verify_table_extents();

            const progress = &forest.progress.?.compact;

            assert(forest.progress.? == .compact);
            const op = forest.progress.?.compact.op;

            const compaction_beat = op % constants.lsm_batch_multiple;
            const last_beat = compaction_beat == constants.lsm_batch_multiple - 1;

            // Finish all our compactions.
            forest.compaction_pipeline.beat_end();

            // Finish loop, runs only on the last beat of every bar, after all async work is done.
            if (last_beat) {
                inline for (0..constants.lsm_levels) |level_b| {
                    inline for (tree_id_range.min..tree_id_range.max) |tree_id| {
                        var tree = tree_for_id(forest, tree_id);
                        assert(tree.compactions.len == constants.lsm_levels);

                        var compaction = &tree.compactions[level_b];
                        compaction.bar_finish(op, tree);
                    }
                }

                assert(forest.compaction_pipeline.bar_active_compactions.count() == 0);
                forest.compaction_pipeline.compactions.clear();
            }

            // Groove sync compaction - must be done after all async work for the beat completes???
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(op);
            }

            // On the last beat of the bar, make sure that manifest log compaction is finished.
            if (last_beat) {
                switch (forest.manifest_log_progress) {
                    .idle => {},
                    .compacting => unreachable,
                    .done => {
                        forest.manifest_log.compact_end();
                        forest.manifest_log_progress = .idle;
                    },
                    .skip => {},
                }
            }

            const callback = progress.callback;
            forest.progress = null;
            callback(forest);
        }

        fn compact_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.manifest_log_progress == .compacting);

            forest.manifest_log_progress = .done;

            if (forest.progress) |progress| {
                assert(progress == .compact);

                forest.compact_callback();
            } else {
                // The manifest log compaction completed between compaction beats.
            }
        }

        fn GrooveFor(comptime groove_field_name: []const u8) type {
            const groove_field = @field(std.meta.FieldEnum(Grooves), groove_field_name);
            return std.meta.FieldType(Grooves, groove_field);
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            assert(forest.manifest_log_progress == .idle);
            forest.grid.assert_only_repairing();
            forest.verify_table_extents();

            forest.progress = .{ .checkpoint = .{ .callback = callback } };

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).assert_between_bars();
            }

            forest.manifest_log.checkpoint(checkpoint_manifest_log_callback);
        }

        fn checkpoint_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest = @fieldParentPtr(Forest, "manifest_log", manifest_log);
            assert(forest.progress.? == .checkpoint);
            assert(forest.manifest_log_progress == .idle);
            forest.grid.assert_only_repairing();
            forest.verify_table_extents();
            forest.verify_tables_recovered();

            const callback = forest.progress.?.checkpoint.callback;
            forest.progress = null;
            callback(forest);
        }

        fn TreeForIdType(comptime tree_id: u16) type {
            const tree_info = tree_infos[tree_id - tree_id_range.min];
            assert(tree_info.tree_id == tree_id);

            return tree_info.Tree;
        }

        pub fn tree_for_id(forest: *Forest, comptime tree_id: u16) *TreeForIdType(tree_id) {
            const tree_info = tree_infos[tree_id - tree_id_range.min];
            assert(tree_info.tree_id == tree_id);

            var groove = &@field(forest.grooves, tree_info.groove_name);

            switch (tree_info.groove_tree) {
                .objects => return &groove.objects,
                .ids => return &groove.ids,
                .indexes => |index_name| return &@field(groove.indexes, index_name),
            }
        }

        pub fn tree_for_id_const(
            forest: *const Forest,
            comptime tree_id: u16,
        ) *const TreeForIdType(tree_id) {
            const tree_info = tree_infos[tree_id - tree_id_range.min];
            assert(tree_info.tree_id == tree_id);

            const groove = &@field(forest.grooves, tree_info.groove_name);

            switch (tree_info.groove_tree) {
                .objects => return &groove.objects,
                .ids => return &groove.ids,
                .indexes => |index_name| return &@field(groove.indexes, index_name),
            }
        }

        /// Verify that `ManifestLog.table_extents` has an extent for every active table.
        ///
        /// (Invoked between beats.)
        fn verify_table_extents(forest: *const Forest) void {
            var tables_count: usize = 0;
            inline for (tree_id_range.min..tree_id_range.max + 1) |tree_id| {
                for (0..constants.lsm_levels) |level| {
                    const tree_level = forest.tree_for_id_const(tree_id).manifest.levels[level];
                    tables_count += tree_level.tables.len();

                    if (constants.verify) {
                        var tables_iterator = tree_level.tables.iterator_from_index(0, .ascending);
                        while (tables_iterator.next()) |table| {
                            assert(forest.manifest_log.table_extents.get(table.address) != null);
                        }
                    }
                }
            }
            assert(tables_count == forest.manifest_log.table_extents.count());
        }

        /// Verify the tables recovered into the ManifestLevels after opening the manifest log.
        ///
        /// There are two strategies to reconstruct the LSM's manifest levels (i.e. the list of
        /// tables) from a superblock manifest:
        ///
        /// 1. Iterate the manifest events in chronological order, replaying each
        ///    insert/update/remove in sequence.
        /// 2. Iterate the manifest events in reverse-chronological order, ignoring events for
        ///    tables that have already been encountered.
        ///
        /// The manifest levels constructed by each strategy are identical.
        ///
        /// 1. This function implements strategy 1, to validate `ManifestLog.open()`.
        /// 2. `ManifestLog.open()` implements strategy 2.
        ///
        /// (Strategy 2 minimizes the number of ManifestLevel mutations.)
        ///
        /// (Invoked immediately after open() or checkpoint()).
        fn verify_tables_recovered(forest: *const Forest) void {
            const ForestTableIteratorType =
                @import("./forest_table_iterator.zig").ForestTableIteratorType;
            const ForestTableIterator = ForestTableIteratorType(Forest);

            assert(forest.grid.superblock.opened);
            assert(forest.manifest_log.opened);

            if (Forest.Storage != @import("../testing/storage.zig").Storage) return;

            // The manifest log is opened, which means we have all of the manifest blocks.
            // But if the replica is syncing, those blocks might still be writing (and thus not in
            // the TestStorage when we go to retrieve them).
            if (forest.grid.superblock.working.vsr_state.sync_op_max > 0) return;

            // The latest version of each table, keyed by table checksum.
            // Null when the table has been deleted.
            var tables_latest = std.AutoHashMap(u128, struct {
                table: schema.ManifestNode.TableInfo,
                manifest_block: u64,
                manifest_entry: u32,
            }).init(forest.grid.superblock.storage.allocator);
            defer tables_latest.deinit();

            // Replay manifest events in chronological order.
            // Accumulate all tables that belong in the recovered forest's ManifestLevels.
            for (0..forest.manifest_log.log_block_checksums.count) |i| {
                const block_checksum = forest.manifest_log.log_block_checksums.get(i).?;
                const block_address = forest.manifest_log.log_block_addresses.get(i).?;
                assert(block_address > 0);

                const block = forest.grid.superblock.storage.grid_block(block_address).?;
                const block_header = schema.header_from_block(block);
                assert(block_header.address == block_address);
                assert(block_header.checksum == block_checksum);
                assert(block_header.block_type == .manifest);

                const block_schema = schema.ManifestNode.from(block);
                assert(block_schema.entry_count > 0);
                assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

                for (block_schema.tables_const(block), 0..) |*table, entry| {
                    if (table.label.event == .remove) {
                        maybe(tables_latest.remove(table.checksum));
                    } else {
                        tables_latest.put(table.checksum, .{
                            .table = table.*,
                            .manifest_block = block_address,
                            .manifest_entry = @intCast(entry),
                        }) catch @panic("oom");
                    }
                }

                if (i > 0) {
                    // Verify the linked-list.
                    const block_previous = schema.ManifestNode.previous(block).?;
                    assert(block_previous.checksum ==
                        forest.manifest_log.log_block_checksums.get(i - 1).?);
                    assert(block_previous.address ==
                        forest.manifest_log.log_block_addresses.get(i - 1).?);
                }
            }

            // Verify that the SuperBlock Manifest's table extents are correct.
            var tables_latest_iterator = tables_latest.valueIterator();
            var table_extent_counts: usize = 0;
            while (tables_latest_iterator.next()) |table| {
                const table_extent = forest.manifest_log.table_extents.get(table.table.address).?;
                assert(table.manifest_block == table_extent.block);
                assert(table.manifest_entry == table_extent.entry);

                table_extent_counts += 1;
            }
            assert(table_extent_counts == forest.manifest_log.table_extents.count());

            // Verify the tables in `tables` are exactly the tables recovered by the Forest.
            var forest_tables_iterator = ForestTableIterator{};
            while (forest_tables_iterator.next(forest)) |forest_table_item| {
                const table_latest = tables_latest.get(forest_table_item.checksum).?;
                assert(table_latest.table.label.level == forest_table_item.label.level);
                assert(std.meta.eql(table_latest.table.key_min, forest_table_item.key_min));
                assert(std.meta.eql(table_latest.table.key_max, forest_table_item.key_max));
                assert(table_latest.table.checksum == forest_table_item.checksum);
                assert(table_latest.table.address == forest_table_item.address);
                assert(table_latest.table.snapshot_min == forest_table_item.snapshot_min);
                assert(table_latest.table.snapshot_max == forest_table_item.snapshot_max);
                assert(table_latest.table.tree_id == forest_table_item.tree_id);

                const table_removed = tables_latest.remove(forest_table_item.checksum);
                assert(table_removed);
            }
            assert(tables_latest.count() == 0);
        }
    };
}
