# Testing

Documentation for (roughly) code in the `src/testing` directory.

## VOPR Output

### Columns

1. Replica index.
2. Event:
    - `!`: crash
    - `^`: recover
    - ` `: commit
    - `$`: sync
    - `[`: checkpoint start
    - `]`: checkpoint done
3. Role (according to the replica itself):
    - `/`: primary
    - `\`: backup
    - `|`: standby
    - `~`: syncing
    - `#`: (crashed)
4. Status:
    - The column (e.g. `.   ` vs `   .`) corresponds to the replica index. (This can help identify events' replicas at a quick glance.)
    - The symbol indicates the `replica.status`.
    - `.`: `normal`
    - `v`: `view_change`
    - `r`: `recovering`
    - `h`: `recovering_head`
    - `s`: `sync`
5. View: e.g. `74V` indicates `replica.view=74`.
6. Checkpoint and Commit: e.g. `83/_90/_98C` indicates that:
   - the highest checkpointed op at the replica is `83` (`replica.op_checkpoint()=83`),
   - on top of that checkpoint, the replica applied ops up to and including `90` (`replica.commit_min=90`),
   - replica knows that ops at least up to `98` are committed in the cluster (`replica.commit_max=98`).
7. Journal op: e.g. `87:150Jo` indicates that the minimum op in the journal is `87` and the maximum is `150`.
8. Journal faulty/dirty: `0/1Jd` indicates that the journal has 0 faulty headers and 1 dirty headers.
9. WAL prepare ops: e.g. `85:149Wo` indicates that the op of the oldest prepare in the WAL is `85` and the op of the newest prepare in the WAL is `149`.
10. Syncing ops: e.g. `<0:123>` indicates that `vsr_state.sync_op_min=0` and `vsr_state.sync_op_max=123`.
11. Release version: e.g. `v1:2` indicates that the replica is running release version `1`, and that its maximum available release is `2`.
12. Grid blocks acquired: e.g. `167Ga` indicates that the grid has `167` blocks currently in use.
13. Grid blocks queued `grid.read_remote_queue`: e.g. `0G!` indicates that there are `0` reads awaiting remote fulfillment.
14. Grid blocks queued `grid_blocks_missing`: e.g. `0G?` indicates that there are `0` blocks awaiting remote repair.
15. Pipeline prepares (primary-only): e.g. `1/4Pp` indicates that the primary's pipeline has 2 prepares queued, out of a capacity of 4.
16. Pipeline requests (primary-only): e.g. `0/3Pq` indicates that the primary's pipeline has 0 requests queued, out of a capacity of 3.

### Example

(The first line labels the columns, but is not part of the actual VOPR output).

```
 1 2 3 4-------- 5---  6----------  7-------  8-----  9------- 10-----   11-- 12-----  13-   14-   15---  16---

 3 [ /    .        3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?   0/4Pp  0/3Rq
 4 ^ \     .       2V  23/_23/_46C  19:_50Jo  0/_0J!  19:_50Wo <__0:__0> v1:2  nullGa  0G!   0G?
 2   \   .         3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
 2 [ \   .         3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
 6   |       .     3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
 6 [ |       .     3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
 3 ] /    .        3V  95/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   167Ga  0G!   0G?   0/4Pp  0/3Rq
 2 ] \   .         3V  95/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   167Ga  0G!   0G?
 1   \  .          3V  71/_99/_99C  68:_99Jo  0/_1J!  67:_98Wo <__0:__0> v1:2   183Ga  0G!   0G?
 1 [ \  .          3V  71/_99/_99C  68:_99Jo  0/_1J!  67:_98Wo <__0:__0> v1:2   183Ga  0G!   0G?
 5   |      .      3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
 5 [ |      .      3V  71/_99/_99C  68:_99Jo  0/_0J!  68:_99Wo <__0:__0> v1:2   183Ga  0G!   0G?
```
