# Tickets for PRD 014: Remove Local Disk Caching

**Source PRD:** docs/prd/014-remove-local-disk-caching.md
**Created:** 2026-03-02
**Total Tickets:** 9
**Estimated Total Complexity:** 21 (S=1, M=2, L=3: 1+1+2+2+3+3+2+2+5=21... recalculate below)

> Complexity sum: T1=S(1) + T2=S(1) + T3=M(2) + T4=M(2) + T5=M(2) + T6=M(2) + T7=M(2) + T8=M(2) + T9=S(1) = **15**

**Estimated Total Complexity:** 15

---

### Ticket 1: Delete `src/storage.rs` and remove module declaration

**Description:**
Delete `src/storage.rs` entirely and remove its `mod storage;` declaration from
`src/lib.rs`. The `StreamLayout` struct is already `#[allow(dead_code)]` and has
no callers outside the module; this is a pure deletion with zero ripple effects.

**Scope:**
- Delete: `src/storage.rs`
- Modify: `src/lib.rs` (remove `mod storage;`)

**Acceptance Criteria:**
- [ ] `src/storage.rs` no longer exists on disk.
- [ ] `src/lib.rs` contains no `mod storage;` or `use crate::storage` reference.
- [ ] Test: `cargo build` compiles without errors or warnings after the deletion.
- [ ] Test: `cargo test` passes — all tests that previously touched `storage` mod
      (the four path-computation tests in the now-deleted file) are absent and no
      other test references `StreamLayout`.
- [ ] Quality gates pass (build, lint `cargo clippy --all-targets -- -D warnings`,
      fmt `cargo fmt --check`).

**Dependencies:** None
**Complexity:** S
**Maps to PRD AC:** AC 1, AC 5

---

### Ticket 2: Relax serde trait bounds on `Aggregate`, `Projection`, `ProcessManager`

**Description:**
Remove `Serialize + DeserializeOwned` from the supertrait bounds of all three
domain traits. These bounds were only needed for snapshot/checkpoint serialization,
which is being deleted. Existing implementors that derive those traits continue to
compile unchanged; the change only widens the set of allowed implementations.

**Scope:**
- Modify: `src/aggregate.rs` (remove `Serialize + DeserializeOwned` from trait def)
- Modify: `src/projection.rs` (remove `Serialize + DeserializeOwned` from trait def;
  remove the `use serde::{Deserialize, Serialize}` import if it becomes unused)
- Modify: `src/process_manager.rs` (remove `Serialize + DeserializeOwned` from trait def;
  clean up unused serde imports on the trait itself — the checkpoint structs and
  `DeadLetterEntry` still use serde, so the imports at the top of the file stay)

**Acceptance Criteria:**
- [ ] `Aggregate` trait bound is `Default + Clone + Send + Sync + 'static` (no serde).
- [ ] `Projection` trait bound is `Default + Clone + Send + Sync + 'static` (no serde).
- [ ] `ProcessManager` trait bound is `Default + Send + Sync + 'static` (no serde).
  Note: `Clone` was on `Projection` but not `ProcessManager` — preserve the status quo.
- [ ] `use serde::de::DeserializeOwned;` is removed from `src/aggregate.rs` (only
  existed for the trait bound; `DomainEvent` associated type still requires it
  separately in the `Aggregate` trait definition, so verify that bound stays).
- [ ] Test: define a struct `NoCopy { data: std::rc::Rc<u32> }` that cannot derive
  Serialize/Deserialize, implement `Aggregate` for it in a `#[cfg(test)]` block in
  `src/aggregate.rs`, and assert it compiles — confirming the bound is removed.
- [ ] Test: `cargo build` and `cargo test` both pass with no new errors.
- [ ] Quality gates pass.

**Dependencies:** None (independent of Ticket 1)
**Complexity:** S
**Maps to PRD AC:** AC 2, AC 3, AC 4

---

### Ticket 3: Delete `src/snapshot.rs` and remove snapshot load/save from `src/actor.rs`

**Description:**
Delete the snapshot module and strip all snapshot-related code from the actor.
The `ActorContext` struct loses its `base_dir` field; `spawn_actor_with_store` and
`spawn_actor_with_config` lose their `base_dir` parameter; the three
`save_snapshot_quietly` call sites in `run_actor` are removed; and the snapshot
tests in the actor module are removed or rewritten. Update `src/lib.rs` to remove
`mod snapshot;`.

**Scope:**
- Delete: `src/snapshot.rs`
- Modify: `src/actor.rs` (remove `base_dir` field from `ActorContext`; remove
  `use crate::snapshot::…` import; remove `base_dir` parameter from
  `spawn_actor_with_config` and `spawn_actor_with_store`; remove all three
  `save_snapshot_quietly` call sites; delete `save_snapshot_quietly` function;
  remove `use std::path::{Path, PathBuf}` if now unused; delete or rewrite the
  two snapshot-specific tests `execute_then_shutdown_saves_snapshot` and
  `spawn_with_snapshot_catches_up_from_server`)
- Modify: `src/lib.rs` (remove `mod snapshot;`)

**Acceptance Criteria:**
- [ ] `src/snapshot.rs` no longer exists on disk and `mod snapshot;` is gone from
  `src/lib.rs`.
- [ ] `ActorContext` struct has no `base_dir` field.
- [ ] `spawn_actor_with_config` and `spawn_actor_with_store` accept no `base_dir`
  parameter.
- [ ] `run_actor` has no calls to `save_snapshot_quietly`; the function itself
  is deleted.
- [ ] All three `ActorMessage::Shutdown`, channel-closed, and idle-timeout arms of
  `run_actor` simply call `break` (or return) without any snapshot I/O.
- [ ] Test: `spawn_actor_with_store` with an empty mock store and a fresh instance
  starts at `stream_version = None`, state = default — asserts that `state()` returns
  the default state.
- [ ] Test: `spawn_actor_with_store` with a pre-seeded mock store (events at versions
  0..2) replays all events from version 0 on spawn, producing the correct derived
  state — confirming full replay from scratch (no snapshot shortcut).
- [ ] Test: idle timeout path — actor shuts down after timeout without panicking
  (no `save_snapshot_quietly` call).
- [ ] Quality gates pass.

**Dependencies:** Ticket 2 (trait bounds relaxed so `Aggregate` no longer requires
`Serialize`; actor generics already use the `A::DomainEvent: DeserializeOwned` bound
on the associated type, which stays)
**Complexity:** M
**Maps to PRD AC:** AC 1, AC 5, AC 9

---

### Ticket 4: Strip projection checkpoints from `src/projection.rs`

**Description:**
Remove all checkpoint I/O from `ProjectionRunner`: delete `ProjectionCheckpoint<P>`,
`save_checkpoint`, `load_checkpoint`; remove `checkpoint_dir: PathBuf` from the
struct; change `ProjectionRunner::new` to take only `(client: EsClient)` (no path);
change `catch_up()` to stop calling `save_checkpoint`; remove the `save()` method
from `ProjectionRunner` and the `save()` method from the `ProjectionCatchUp` trait.
Track position in a plain `last_global_position: u64` field (starts at 0).
Delete or replace all checkpoint-related tests in `src/projection.rs`.

**Scope:**
- Modify: `src/projection.rs` (delete `ProjectionCheckpoint`, `save_checkpoint`,
  `load_checkpoint`; simplify `ProjectionRunner` fields to `state: P`,
  `last_global_position: u64`, `client`; update `new`, `catch_up`, `apply_event`,
  `position`, `state_any`; remove `save()` from both runner and trait; remove
  `checkpoint_dir: PathBuf` field; update `use` imports; rewrite tests)

**Acceptance Criteria:**
- [ ] `ProjectionCheckpoint<P>` struct does not exist.
- [ ] `save_checkpoint` and `load_checkpoint` functions do not exist in
  `src/projection.rs`.
- [ ] `ProjectionRunner::new` signature is `fn new(client: EsClient) -> Self`
  (infallible — no `io::Result` needed since no file load).
- [ ] `ProjectionCatchUp` trait has no `save` method.
- [ ] `ProjectionRunner` has no `checkpoint_dir` field.
- [ ] `catch_up()` does not write any file; it only subscribes from
  `self.last_global_position`, processes events, and returns `io::Result<()>`.
- [ ] `process_stream` still correctly advances `last_global_position` after each event.
- [ ] Test: construct a `ProjectionRunner`, call `process_stream` with a 2-event
  mock stream + `CaughtUp`, assert `last_global_position == 2` and state is correct.
- [ ] Test: second call to `process_stream` with only `CaughtUp` leaves state and
  position unchanged.
- [ ] Test: `apply_event` on a live-mode runner advances position by 1 and updates
  state.
- [ ] Test: `position()` returns 0 on a freshly constructed runner.
- [ ] Test: no test references `save_checkpoint`, `load_checkpoint`, or
  `checkpoint_dir` anywhere in the file.
- [ ] Quality gates pass.

**Dependencies:** Ticket 2 (serde bound removed from `Projection` trait; the test
fixture `EventCounter` still derives Serialize/Deserialize if desired but is not
required to)
**Complexity:** M
**Maps to PRD AC:** AC 3, AC 5, AC 9

---

### Ticket 5: Strip process manager checkpoints from `src/process_manager.rs`

**Description:**
Remove checkpoint I/O from `ProcessManagerRunner` while preserving dead-letter
functionality. Delete `ProcessManagerCheckpoint<PM>`, `save_pm_checkpoint`,
`load_pm_checkpoint`; remove `checkpoint_dir` field (rename to `data_dir` — the
dead-letter root); change `ProcessManagerRunner::new` to take `(client: EsClient,
data_dir: PathBuf)` (infallible); remove the `save()` method from both the runner
and `ProcessManagerCatchUp` trait. Track position in a plain `last_global_position:
u64` field (starts at 0). `dead_letter_path()` returns `self.data_dir.join("dead_letters.jsonl")`.
Delete or replace all checkpoint persistence tests while keeping dead-letter tests.

**Scope:**
- Modify: `src/process_manager.rs` (delete `ProcessManagerCheckpoint`, `save_pm_checkpoint`,
  `load_pm_checkpoint`; simplify `ProcessManagerRunner` fields to `state: PM`,
  `last_global_position: u64`, `client`, `data_dir: PathBuf`; update `new`,
  `catch_up`, `react_event`, `position`, `name`, `dead_letter_path`; remove `save()`
  from runner and trait; clean up unused serde imports from the top of the file if
  applicable; rewrite tests)

**Acceptance Criteria:**
- [ ] `ProcessManagerCheckpoint<PM>` struct does not exist.
- [ ] `save_pm_checkpoint` and `load_pm_checkpoint` do not exist.
- [ ] `ProcessManagerRunner::new` signature is
  `fn new(client: EsClient, data_dir: PathBuf) -> Self` (infallible).
- [ ] `ProcessManagerCatchUp` trait has no `save` method.
- [ ] `ProcessManagerRunner` has no `checkpoint_dir` field; the dead-letter root
  is stored in `data_dir`.
- [ ] `dead_letter_path()` still returns `self.data_dir.join("dead_letters.jsonl")`.
- [ ] `catch_up()` does not write any file.
- [ ] `append_dead_letter` function is preserved and unchanged.
- [ ] Test: construct a runner, call `process_stream` with one event + `CaughtUp`,
  assert one envelope returned, `last_global_position == 1`, `state.events_seen == 1`.
- [ ] Test: second pass with only `CaughtUp` returns empty envelopes, state unchanged.
- [ ] Test: `dead_letter_append_creates_readable_jsonl` test still passes with the
  new constructor (no checkpoint dir required).
- [ ] Test: no test references `save_pm_checkpoint`, `load_pm_checkpoint`, or `checkpoint_dir`.
- [ ] Quality gates pass.

**Dependencies:** Ticket 2 (serde bound removed from `ProcessManager` trait)
**Complexity:** M
**Maps to PRD AC:** AC 4, AC 5, AC 6, AC 9

---

### Ticket 6: Update `src/actor.rs` callers and `src/store.rs` for removed `base_dir` parameter

**Description:**
`spawn_actor_with_config` (from Ticket 3) no longer takes `base_dir`. Update the
call site in `src/store.rs` where `spawn_actor_with_config` is called inside
`AggregateStore::get`. The `AggregateStore` struct still keeps its `base_dir` field
(used for computing dead-letter paths via `ProcessManagerRunner`), but the actor
spawn call no longer passes it. Also update `src/store.rs` factory closures for
`ProjectionRunner::new` and `ProcessManagerRunner::new` to match the new simplified
signatures from Tickets 4 and 5 (drop `checkpoint_dir`, pass `data_dir` instead).
Remove the `pm.save()` call in `run_process_managers` since the `save()` method no
longer exists. Update `src/store.rs` tests that construct mock `ProjectionRunner` or
`ProcessManagerRunner` directly.

**Scope:**
- Modify: `src/store.rs` (update `AggregateStore::get` spawn call; update
  `AggregateStoreBuilder::projection` factory closure; update
  `AggregateStoreBuilder::process_manager` factory closure; remove `pm.save()` calls
  from `run_process_managers`; update tests)

**Acceptance Criteria:**
- [ ] `AggregateStore::get` calls `spawn_actor_with_config(id, self.client.clone(), config)`
  with no `base_dir` argument.
- [ ] The `AggregateStoreBuilder::projection` factory calls
  `ProjectionRunner::<P>::new(client)` with no path argument.
- [ ] The `AggregateStoreBuilder::process_manager` factory calls
  `ProcessManagerRunner::<PM>::new(client, data_dir)` where `data_dir =
  base_dir.join("process_managers").join(PM::NAME)`.
- [ ] `run_process_managers` does not call `pm.save()` after dispatch — the post-dispatch
  checkpoint save loop is deleted.
- [ ] `ProjectionFactory` type alias no longer needs `&Path` if projections don't use it;
  if the factory type still receives `base_dir` for PM paths, ensure only PM factories
  use it.
- [ ] Test: `AggregateStoreBuilder` doc example in comments compiles (`no_run`).
- [ ] Test: `run_process_managers` test (using mock store) verifies that dead-lettering
  still works and no `save()` call is made.
- [ ] Test: `mock_store_with_projection` and `mock_store_with_projection_and_pm` helper
  functions in `src/store.rs` (and `src/live.rs` tests) updated to use new constructors.
- [ ] Quality gates pass.

**Dependencies:** Ticket 3 (actor `base_dir` param removed), Ticket 4 (projection
`new` signature changed), Ticket 5 (PM `new` signature changed, `save()` gone)
**Complexity:** M
**Maps to PRD AC:** AC 5, AC 7, AC 9

---

### Ticket 7: Simplify `src/live.rs` — remove checkpoint timer and `save_all_checkpoints`

**Description:**
Remove `LiveConfig::checkpoint_interval` and all checkpoint-saving logic from the
live loop. The `save_all_checkpoints` function is deleted. The periodic
`checkpoint_interval.tick()` timer arm in `run_live_loop`'s select loop is removed.
All call sites of `save_all_checkpoints` (shutdown, reconnect, stream-error, and
stream-end paths) are removed. The `ProjectionCatchUp` and `ProcessManagerCatchUp`
traits no longer have a `save()` method (done in Tickets 4 and 5), so the loop
body simply fans out events and dispatches PM envelopes without any disk I/O.
Update `LiveConfig::default()` and all tests that reference `checkpoint_interval`.

**Scope:**
- Modify: `src/live.rs` (delete `save_all_checkpoints`; remove
  `checkpoint_interval` timer from `run_live_loop`; remove all `save_all_checkpoints`
  call sites; simplify `run_live_loop` select loop; remove `use std::path::PathBuf`
  if unused; update `LiveConfig` doc comment; update tests)

**Acceptance Criteria:**
- [ ] `LiveConfig` struct has no `checkpoint_interval` field.
- [ ] `LiveConfig::default()` only sets `reconnect_base_delay` and `reconnect_max_delay`.
- [ ] `save_all_checkpoints` function does not exist anywhere in the crate
  (`grep -r save_all_checkpoints` returns no matches).
- [ ] `run_live_loop` has no `tokio::time::interval(…)` timer.
- [ ] The select loop inside `run_live_loop` has exactly two arms: `stream_fut`
  result and `shutdown_rx.changed()`.
- [ ] `LiveConfig` doc comment example in the public API no longer references
  `checkpoint_interval`.
- [ ] Test: `live_config_default_values` test updated — asserts only
  `reconnect_base_delay` and `reconnect_max_delay` fields exist with correct defaults.
- [ ] Test: `live_loop_saves_checkpoints_on_shutdown` test is deleted (its premise
  — that shutdown triggers a checkpoint save — is no longer valid).
- [ ] Test: `run_live_loop_shutdown_saves_checkpoints` test is deleted.
- [ ] Test: helper `mock_store_with_projection` updated to construct
  `ProjectionRunner::new(client)` (no path) per Ticket 4's simplified constructor.
- [ ] Test: `live_loop_processes_events_and_fans_out_to_projections` still passes.
- [ ] Quality gates pass.

**Dependencies:** Ticket 4 (`ProjectionCatchUp::save` gone), Ticket 5
(`ProcessManagerCatchUp::save` gone), Ticket 6 (`store.rs` constructors updated
so that `mock_store_with_projection` in `live.rs` tests compiles)
**Complexity:** M
**Maps to PRD AC:** AC 5, AC 8, AC 9

---

### Ticket 8: Update `Cargo.toml` doc comments and verify `serde` dependency retention

**Description:**
Verify that `serde` and `serde_json` stay in `Cargo.toml` (they are still used for
`DeadLetterEntry`, `CommandEnvelope`, `EventMetadata`, `StoredEvent`, etc.) and
remove any doc comments or rustdoc examples in the public API that reference the
removed `checkpoint_interval`, snapshot paths, or the deleted modules. Update
`src/lib.rs` module-level doc comment to remove references to "local caches" and
"snapshots/checkpoints". Update `AggregateStoreBuilder::base_dir` doc comment to
clarify its remaining purpose (dead-letter path root only). Update
`AggregateStoreBuilder::idle_timeout` doc comment to remove the snapshot reference.

**Scope:**
- Modify: `Cargo.toml` (no version changes needed; verify no dependency removal)
- Modify: `src/lib.rs` (update crate-level and module doc comment)
- Modify: `src/store.rs` (update `base_dir`, `idle_timeout`, and builder doc
  comments; remove snapshot mentions)

**Acceptance Criteria:**
- [ ] `serde` and `serde_json` remain in `[dependencies]` in `Cargo.toml`.
- [ ] `cargo doc --no-deps` builds without warnings.
- [ ] No public doc comment refers to "snapshot", "checkpoint_interval",
  "snapshots/", or "checkpoints/" in user-facing documentation.
- [ ] `AggregateStoreBuilder::base_dir` doc comment states it is the root for
  dead-letter file paths.
- [ ] `AggregateStoreBuilder::idle_timeout` doc comment no longer says "save a
  snapshot" — updated to simply describe actor eviction behavior.
- [ ] Test: `cargo doc --no-deps 2>&1 | grep -i "warning"` returns no output.
- [ ] Test: grep for `"snapshot"` in public doc comments (lines starting with `///`)
  across `src/store.rs` and `src/lib.rs` returns no matches.
- [ ] Quality gates pass.

**Dependencies:** Tickets 1–7 (all removals complete so doc comments reflect final state)
**Complexity:** M
**Maps to PRD AC:** AC 5, AC 8, AC 10

---

### Ticket 9: Verification and integration test

**Description:**
Run the full PRD acceptance criteria checklist. Verify that `src/snapshot.rs` and
`src/storage.rs` are deleted, no file writes occur for any cache path, dead-letter
writing is intact, `LiveConfig` has no `checkpoint_interval`, trait bounds are
relaxed, and the test suite is green with clean lint and formatting.

**Acceptance Criteria:**
- [ ] `src/snapshot.rs` does not exist:
  `test ! -f src/snapshot.rs` exits 0.
- [ ] `src/storage.rs` does not exist:
  `test ! -f src/storage.rs` exits 0.
- [ ] No reference to deleted modules in `src/lib.rs`:
  `grep -n "mod snapshot\|mod storage" src/lib.rs` returns no matches.
- [ ] Trait bounds are relaxed — no serde supertraits on `Aggregate`, `Projection`,
  `ProcessManager`:
  `grep -n "Serialize\|DeserializeOwned" src/aggregate.rs src/projection.rs src/process_manager.rs`
  returns matches only on `DomainEvent` associated type bounds and internal
  checkpoint/dead-letter structs (not on the trait definition lines).
- [ ] No snapshot or checkpoint file I/O exists anywhere:
  `grep -rn "snapshot\.json\|checkpoint\.json\|snapshots/\|projections/checkpoint"
  src/` returns no matches.
- [ ] Dead-letter path still works:
  `grep -n "dead_letters\.jsonl\|append_dead_letter" src/process_manager.rs`
  returns at least one match on each.
- [ ] `base_dir` remains on `AggregateStore`:
  `grep -n "base_dir" src/store.rs` returns at least one match (the field).
- [ ] `LiveConfig` has no `checkpoint_interval`:
  `grep -n "checkpoint_interval" src/live.rs` returns no matches.
- [ ] `save_all_checkpoints` does not exist:
  `grep -rn "save_all_checkpoints" src/` returns no matches.
- [ ] All existing tests pass: `cargo test` exits 0.
- [ ] No regressions in existing tests.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` exits 0.
- [ ] `cargo fmt --check` exits 0.

**Dependencies:** All previous tickets (1–8)
**Complexity:** S
**Maps to PRD AC:** AC 1–10

---

## AC Coverage Matrix

| PRD AC # | Description | Covered By Ticket(s) | Status |
|----------|-------------|----------------------|--------|
| 1 | `src/snapshot.rs` and `src/storage.rs` deleted and not referenced in `src/lib.rs` | Ticket 1, Ticket 3, Ticket 9 | Covered |
| 2 | `Aggregate` trait bound has no `Serialize` or `DeserializeOwned` | Ticket 2, Ticket 9 | Covered |
| 3 | `Projection` trait bound has no `Serialize` or `DeserializeOwned` | Ticket 2, Ticket 4, Ticket 9 | Covered |
| 4 | `ProcessManager` trait bound has no `Serialize` or `DeserializeOwned` | Ticket 2, Ticket 5, Ticket 9 | Covered |
| 5 | No code path creates/reads/writes `snapshot.json`, `checkpoint.json`, or any `snapshots/` or `projections/` subdirectory | Ticket 1, Ticket 3, Ticket 4, Ticket 5, Ticket 7, Ticket 9 | Covered |
| 6 | Dead-letter file writing (`append_dead_letter` to `dead_letters.jsonl`) preserved and unchanged | Ticket 5, Ticket 6, Ticket 9 | Covered |
| 7 | `base_dir` remains available on `AggregateStore` for dead-letter path computation | Ticket 6, Ticket 8, Ticket 9 | Covered |
| 8 | `LiveConfig` does not contain `checkpoint_interval`; `save_all_checkpoints` does not exist | Ticket 7, Ticket 8, Ticket 9 | Covered |
| 9 | `cargo test` passes with all existing tests updated for new behavior | Ticket 3, Ticket 4, Ticket 5, Ticket 6, Ticket 7, Ticket 9 | Covered |
| 10 | `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt --check` both exit 0 | Ticket 9 | Covered |
