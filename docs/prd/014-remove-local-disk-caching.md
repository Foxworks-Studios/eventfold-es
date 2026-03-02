# PRD 014: Remove Local Disk Caching

**Status:** TICKETS READY
**Created:** 2026-03-02
**Author:** PRD Writer Agent

---

## Problem Statement

`eventfold-es` maintains three forms of local disk caching -- aggregate
snapshots, projection checkpoints, and process manager checkpoints -- to
accelerate replay by skipping already-processed events. In practice, these
caches are a persistent source of state-drift bugs: whenever event schemas
change, `apply`/`react` logic is updated, or events are injected/replayed in
a different order, the cached state silently diverges from what a full replay
would produce. Debugging these inconsistencies is time-consuming and the
caching itself is premature optimization for the current scale of the project.

Removing all three caching mechanisms simplifies the codebase, eliminates an
entire class of bugs, and reduces the trait-bound surface area required of
user-defined aggregates, projections, and process managers.

## Goals

- Delete all aggregate snapshot code (`src/snapshot.rs`) and the snapshot
  save/load calls in `src/actor.rs`.
- Delete all projection checkpoint persistence (`save_checkpoint`,
  `load_checkpoint` in `src/projection.rs`) and the checkpoint file I/O in
  `ProjectionRunner`.
- Delete all process manager checkpoint persistence (`save_pm_checkpoint`,
  `load_pm_checkpoint` in `src/process_manager.rs`) and the checkpoint file I/O
  in `ProcessManagerRunner`.
- Delete `src/storage.rs` (the `StreamLayout` path-computation helper, which is
  already `#[allow(dead_code)]`).
- Remove `LiveConfig::checkpoint_interval` and the periodic `save_all_checkpoints`
  timer in the live loop.
- Relax trait bounds: remove `Serialize + DeserializeOwned` from `Aggregate`,
  `Projection`, and `ProcessManager` traits.
- Ensure projections and process managers always replay from global position 0 on
  startup, and aggregates always replay the full stream on actor spawn.

## Non-Goals

- Introducing an alternative caching strategy (e.g. in-memory LRU, Redis).
- Removing dead-letter file persistence -- dead-letter JSONL files are a durable
  audit trail, not a read-acceleration cache.
- Changing the `base_dir` field on `AggregateStore` -- it is still needed as the
  root for dead-letter file paths.
- Removing reconnection logic or exponential backoff in `src/live.rs`.
- Changing the gRPC protocol or `eventfold-db` server behavior.

## Technical Approach

### Phase 1: Delete snapshot module

Remove `src/snapshot.rs` entirely. Remove `mod snapshot;` from `src/lib.rs`.
Remove the `load_snapshot` call in `spawn_actor_with_store` (actors always replay
from stream version 0) and the `save_snapshot_quietly` call in `run_actor`'s
shutdown path. Remove the `Snapshot` struct import and any snapshot-related fields
from `ActorContext`.

### Phase 2: Relax trait bounds

Remove `Serialize + DeserializeOwned` from:

- `Aggregate` trait bound (`src/aggregate.rs`)
- `Projection` trait bound (`src/projection.rs`)
- `ProcessManager` trait bound (`src/process_manager.rs`)

This is a **breaking change** for downstream crates that rely on these
supertraits. However, removing a bound is strictly additive from the
implementor's perspective -- existing types that derive `Serialize`/`Deserialize`
will continue to compile; types that previously could not implement these traits
due to non-serializable fields now can.

Remove any `use serde::{Serialize, Deserialize}` imports that become unused after
the bound removal.

### Phase 3: Strip projection checkpoints

In `src/projection.rs`:

- Delete `ProjectionCheckpoint<P>` struct.
- Delete `save_checkpoint` and `load_checkpoint` functions.
- Remove `checkpoint_dir: PathBuf` from `ProjectionRunner`.
- Change `ProjectionRunner::new` to initialize with `last_global_position: 0`
  unconditionally (no file load).
- Remove the `save()` method from `ProjectionRunner` and the `save()` method
  from the `ProjectionCatchUp` trait.
- `catch_up()` no longer calls `save_checkpoint` after catching up.

### Phase 4: Strip process manager checkpoints

In `src/process_manager.rs`:

- Delete `ProcessManagerCheckpoint<PM>` struct.
- Delete `save_pm_checkpoint` and `load_pm_checkpoint` functions.
- Change `ProcessManagerRunner::new` to initialize with
  `last_global_position: 0` unconditionally (no file load).
- Remove the `save()` method from `ProcessManagerRunner` and the `save()`
  method from the `ProcessManagerCatchUp` trait.
- Keep `dead_letter_path()` and `append_dead_letter` unchanged. The
  `checkpoint_dir` field may be renamed to `data_dir` or similar for clarity,
  since it now only roots the dead-letter file.

### Phase 5: Simplify actor module

In `src/actor.rs`:

- Remove the `base_dir` field from `ActorContext` if it was only used for
  snapshot save/load.
- Remove any snapshot-related imports.
- `spawn_actor_with_store` always reads the full stream starting at version 0.

### Phase 6: Simplify store module

In `src/store.rs`:

- Remove `base_dir` from `AggregateStoreBuilder` if it is no longer needed by
  actor spawn. If `base_dir` is still needed for dead-letter paths, keep it but
  remove the `snapshots` and `projections` subdirectory computations.
- Remove the `checkpoint_dir` arguments passed to `ProjectionRunner::new` and
  `ProcessManagerRunner::new` if those runners no longer accept them. Pass only
  the dead-letter root to process manager runners.
- In `run_process_managers`, remove the post-dispatch `pm.save()` call.

### Phase 7: Simplify live loop

In `src/live.rs`:

- Remove `LiveConfig::checkpoint_interval` field and its `Default` value.
- Remove the `tokio::time::interval(config.checkpoint_interval)` timer.
- Remove the `save_all_checkpoints` function and all calls to it (periodic tick,
  graceful shutdown, reconnect).
- The live loop continues to track `last_global_position` in memory for
  reconnection (subscribe from the last seen position), but does not persist it
  to disk.

### Phase 8: Delete storage module

Remove `src/storage.rs` entirely. Remove `mod storage;` from `src/lib.rs`.

### File-change table

| File | Change |
|------|--------|
| `src/snapshot.rs` | Delete |
| `src/storage.rs` | Delete |
| `src/lib.rs` | Remove `mod snapshot;` and `mod storage;` |
| `src/aggregate.rs` | Remove `Serialize + DeserializeOwned` from trait bound |
| `src/projection.rs` | Remove checkpoint structs/functions, remove serde bounds, remove `save()` |
| `src/process_manager.rs` | Remove checkpoint structs/functions, remove serde bounds, keep dead-letter |
| `src/actor.rs` | Remove snapshot load/save, always replay full stream |
| `src/store.rs` | Remove snapshot/checkpoint dir setup, remove `save()` calls, keep dead-letter `base_dir` |
| `src/live.rs` | Remove `checkpoint_interval`, remove `save_all_checkpoints`, keep reconnect position tracking |
| `Cargo.toml` | Remove `serde` from public dependency if no longer needed in trait bounds (keep if used internally for dead-letter serialization) |

## Breaking Changes

This PRD introduces the following breaking changes to the public API:

1. **Trait bound relaxation**: `Serialize + DeserializeOwned` removed from
   `Aggregate`, `Projection`, and `ProcessManager`. Existing implementations
   that derive `Serialize`/`Deserialize` continue to compile, but code that
   relied on these supertraits (e.g. serializing an `impl Aggregate` generically)
   will need explicit bounds.
2. **`LiveConfig::checkpoint_interval` removed**: Callers that set this field
   will get a compile error. Since `LiveConfig` has no other cache-related
   fields, this is a straightforward removal.
3. **Startup behavior change**: Projections and process managers always replay
   from position 0 on startup. Aggregates always replay the full stream on
   actor spawn. This is functionally correct but slower for large event stores.
   Document this trade-off in the changelog.

## Acceptance Criteria

1. `src/snapshot.rs` and `src/storage.rs` are deleted and no longer referenced
   in `src/lib.rs`.
2. The `Aggregate` trait bound in `src/aggregate.rs` does not include `Serialize`
   or `DeserializeOwned`.
3. The `Projection` trait bound in `src/projection.rs` does not include
   `Serialize` or `DeserializeOwned`.
4. The `ProcessManager` trait bound in `src/process_manager.rs` does not include
   `Serialize` or `DeserializeOwned`.
5. No code path in the crate creates, reads, or writes `snapshot.json`,
   `checkpoint.json`, or any file under a `snapshots/` or `projections/`
   subdirectory.
6. Dead-letter file writing (`append_dead_letter` to `dead_letters.jsonl`) is
   preserved and unchanged.
7. `base_dir` remains available on `AggregateStore` for dead-letter path
   computation.
8. `LiveConfig` does not contain a `checkpoint_interval` field.
   `save_all_checkpoints` does not exist.
9. `cargo test` passes with all existing tests updated for the new behavior.
10. `cargo clippy --all-targets --all-features -- -D warnings` and
    `cargo fmt --check` both exit with code 0.

## Open Questions

- Should `base_dir` be renamed to `data_dir` or `dead_letter_dir` to reflect
  its reduced scope? This is a cosmetic change that could be bundled or deferred.
- Should the `Projection` trait keep the `Clone` bound? It was useful for
  checkpoint serialization but may still be needed for other reasons (e.g.
  returning a snapshot of projection state to callers via `state()`).

## Dependencies

- No new crate dependencies required.
- `serde` and `serde_json` may remain as dependencies if used internally for
  dead-letter serialization (`DeadLetterEntry`) or event deserialization, but
  they are no longer part of the public trait API.
