# V4 Flink sink POC, iteration 4: failure scenarios and resource comparison (local only, 2026-11-13)

Branch `poc-flink-index-dv-writer` (base `e78048745`), commits `b203eb7ee` (harnesses), `195798a8b` (D fail-fast check), `a8c5263d6` (compaction cases), `ec3a5b54e` (DV-rewrite commit fix), `4ee433707` (rebuild option), `29cd08b52` (concurrent compaction cases) and `ccb3dac42` (rebase fix), not pushed. All runs are in-JVM: a
MiniCluster with HadoopCatalog on the local filesystem, Flink 2.3, V3 tables, parallelism 2, `--max-workers=2`, 1.5 GB
test heap (build default). Laptop numbers, rough.

Modes:

- **A**: append only (no upsert). This is the baseline for replay behaviour.
- **B**: upsert with equality deletes, today's default.
- **C**: B plus the in-job equality delete converter (`convertEqualityDeletes`, eq → DV after commit, #18717 line).
- **D**: `upsert-mode=index-dv` from this POC (keyed RocksDB index, DVs at write time, `IcebergSink`).

## Part 1: correctness under failure

**Harness:** `TestIcebergSinkFailureScenarios`.

- The mode comes from `FAILURE_MODE`.
- Each scenario uses one fresh MiniCluster per job run, so a fixed job id can be submitted again.
- The source emits record `i` as `(i % 60, "v" + i)` for 400 records. It emits one 40-record batch per completed
  checkpoint, from a checkpointed offset, so a restore replays exactly what follows the restored offset.
- Checkpoint interval is 200 ms. 50 checkpoints are retained (cluster config).
- Each test prints one `RESULT` line and asserts nothing, so a wrong mode still reports.

Expected rows: 400 for A, 60 (one per key, last value) for B, C and D.

| # | scenario | A (exp. 400) | B (exp. 60) | C (exp. 60) | D (exp. 60) |
|---|---|---|---|---|---|
| s1 | failover to the latest checkpoint, input flowing | 400 ✔ | 60 ✔, eq 20 | 60 ✔, eq 20 (18 live) | 60 ✔, DV 18 |
| s2 | restore a retained checkpoint 2+ commits behind the table | **520, 120 dup** (silent) | 60 ✔, eq 26 | 60 ✔, eq 24 (18 live), DV 6 | **120: 60 stale rows + 60 dup keys** (silent) |
| s3 | restore a savepoint taken before 5 more commits | **600, 200 dup** (silent) | 60 ✔, eq 30 | 60 ✔, eq 30 (26 live) | **120: 60 stale rows + 60 dup keys** (silent) |
| s4 | stateless restart, same fixed job id (#18098) | 400 ✔ | 60 ✔ | 60 ✔ | 60 ✔ |
| s5a | different job id restores a stop-with-savepoint | 400 ✔ | 60 ✔ | 60 ✔ | 60 ✔ |
| s5b | two jobs continue from one savepoint, one after the other | **600, 200 dup** (silent) | 60 ✔, eq 22 | 60 ✔, eq 28 (24 live) | **120: 60 identical dup rows** (silent) |
| s6a | cancel during a blocked checkpoint, restore the latest completed one | 400 ✔ | 60 ✔ | 60 ✔ (2 of 3 runs, see the note) | 60 ✔ |
| s6b | cancel during a blocked checkpoint, restart with no state | **600, 200 dup** (silent) | 60 ✔, eq 30 | 60 ✔ | 60 ✔, DV 26 |

`eq` counts the equality delete files ever committed. `DV` counts live DVs.

- **Dangling DVs** (a DV on a removed data file): 0 in every run, in every mode.
- **Missing rows:** 0 in every run, in every mode. No scenario lost data.
- **D never writes equality deletes.**
- **C leaves most equality delete files live** at the end of every scenario. The converter only converts what it
  has planned by the time the job ends, so a short job keeps most of its eq deletes.

**How the scenarios were simulated**

- s2 ("crash in the commit window"): the job runs past checkpoint k and commits k+1 and k+2, then is cancelled.
  It is then restored from retained checkpoint k. A real SIGKILL is not possible in-JVM; cancel stands in for it.
  The effect on the table is the same: snapshots exist that the restored state has never seen.
- s4 pins the job id with `PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID`. The #18098 loss (the V2 committer
  skipping a fresh job's first checkpoint ids) does not reproduce on this base. Main's `IcebergCommitter` now
  guards with `isRestored`.

**Loud vs silent** (before the fail-fast check, see below). Every wrong result is silent: the jobs finish successfully and the table holds wrong data.

- **A** is wrong wherever a replay crosses already-committed data (s2, s3, s5b, s6b). That is the known at-least-once
  behaviour of append under these misuses.
- **D** is wrong in s2, s3 and s5b.

The only loud failure is the C flake below.

**C flake (loud).** In one run of s6a, the job failed before the cancel:

```
java.lang.IllegalArgumentException: Lock time is null, Can't release lock
```

It was thrown in the `EqConvert Planner` (TriggerManager) task, with restart strategy `none`. Two reruns of s6a
alone were clean. It looks like the lock-release path in the maintenance `TriggerManager` (the #18435 / #18439
area). It was not investigated further here.

### Root cause for D (s2, s3, s5b)

This is a hypothesis. It is consistent with every count above, but has not been confirmed with a debugger.

The keyed index stores, per key, the live position (file, pos) as of the restored checkpoint or savepoint. After a
restore from state that is older than the table head:

1. The rows that the abandoned run committed after that state are live in the table, but are not in the index.
   This covers rows written by a sibling job in s5b.
2. When the replay upserts those keys, the writer DVs the old position the index knows about. It never DVs the
   newer row. Result: one stale live row plus one new row per key (s2, s3: 60 stale + 60 new).
   - In s5b the values are identical, so 60 exact duplicates.
3. Nothing catches it:
   - `applyRewrites()` reconciles only `replace` snapshots (compaction), so foreign or later `append` /
     `overwrite` snapshots are ignored.
   - `IndexDvCommits.validationStart` finds this sink's own `operatorId` snapshot, even when it comes from the
     abandoned lineage. Commit validation therefore starts after the rows it should be checking.

B and C are immune because an equality delete removes every older row with the key, wherever it is.

D is correct with no state (s4, s6b), because it bootstraps the index from the head. It is also correct with state
that matches the head (s1, s5a, s6a).

**Proposed fix.** Pick one, in order of preference.

1. **Reconcile on restore.** Keep `lastSeenSnapshotId` in operator state. On restore:
   - walk the table from it to the head;
   - for every snapshot not produced by this restored lineage (identified by the job's `flink.job-id`,
     operator id and checkpoint id in the summary), apply the added data files' key columns plus `_pos` to the
     index as upserts;
   - for removed files, drop their positions.

   This is the same delta as the rewrite handling, extended to `append` and `overwrite`, and the same approach as
   the converter's index catch-up (#18717). Cost: it reads the key columns of everything committed since the
   state was taken.
2. **Re-bootstrap when the state is behind.** If the head is not `lastSeenSnapshotId`, or a descendant of it via
   only this lineage's commits, discard keyed state and bootstrap from the head. This is simple and always
   correct, but costs a full bootstrap (about 11 s at 10M keys here).
3. **Fail fast.** Refuse to start when the restored state is behind the head and an option such as
   `index-dv.allow-stale-restore` is not set. The failure becomes loud; the user chooses option 2.

The minimum for a first upstream version is option 3 plus option 2 behind a flag. Option 1 is the follow-up.

### Fail-fast check implemented (commit `195798a8b`)

Option 3 of the proposed fix is implemented, on its own commit so it can be reverted. Options 1 and 2, including
the rebuild flag, are not implemented.

**What it checks.** On restore, each `IndexDvDataWriter` subtask walks the branch from the head back to
`lastSeenSnapshotId`. That is the head at the last barrier, already kept in union operator state. For every
snapshot that is not a `replace` (compaction), it checks each added data file against the restored file
dictionary. The dictionary is union state and lists every data file whose positions the index can hold:

- bootstrapped files;
- files this sink wrote up to the restored checkpoint, committed or not;
- rewrite outputs.

Any unknown file fails the job with an `IllegalStateException`. The message names:

- the table and branch;
- the snapshot the index reflects, and the head;
- the offending snapshot, with its operation, `flink.job-id` and `flink.max-committed-checkpoint-id`;
- the file;
- the remedy: start without state so the index bootstraps from the head, or restore the checkpoint or savepoint
  that matches the head.

It also fails loudly when the restored snapshot is not an ancestor of the head, or its history was expired.

**Why file membership instead of operator id and checkpoint id.** Membership gives the same answer as "same
operator id and `max-committed-checkpoint-id` ≤ the restored checkpoint id" for the sink's own commits. It also:

- needs no job id or operator id plumbing into the writer (the summary's operator id is the aggregator's);
- does not depend on the restored job keeping the old job id (s5a restores under a new one);
- catches a second job that reuses the same operator id (s5b).

A committed but unacknowledged checkpoint is the normal-failover case. Its files were written before the
restored barrier, so they are in the dictionary and the commit passes.

**Results, mode D, re-run twice on the final code:**

| scenario | before (`b203eb7ee`) | with the check (`195798a8b`) |
|---|---|---|
| s1 failover to the latest checkpoint | 60 ✔ | 60 ✔ |
| s2 checkpoint behind the table | 120: 60 stale + 60 dup keys, silent | **fails loudly at restore**: table left at the abandoned run's 60 rows, no dup keys |
| s3 savepoint 5 commits old | 120: 60 stale + 60 dup keys, silent | **fails loudly at restore**: table left at 60 rows, no dup keys |
| s4 stateless, same job id | 60 ✔ | 60 ✔ |
| s5a other job restores the savepoint | 60 ✔ | 60 ✔ |
| s5b two jobs from one savepoint | 120: 60 identical dups, silent | **fails loudly at restore**: table left at job 1's correct 60 rows |
| s6a cancel in checkpoint, restore latest | 60 ✔ | 60 ✔ |
| s6b cancel in checkpoint, no state | 60 ✔ | 60 ✔ |

- In s2 and s3 the `unexpectedRows=60 missing=60` count is not corruption. The replay never ran, so each key still
  holds the abandoned run's last value. Restarting without state produces the expected 60 (s6b shows that path).
- The offending snapshots named in the errors are this sink's own later commits: `overwrite`,
  `max-committed-checkpoint-id` 11, 13 and 18, from the abandoned lineage or the sibling job.

**Regression:**

- `TestIcebergSinkIndexDv`: 84 run, 0 failures, 38 skipped by design.
- A new test, `restoreFailsWhenTableHasCommitsTheIndexHasNotSeen`, covers an external append after a
  stop-with-savepoint. The restore fails with the message, and the table is left untouched.
- `savepointRestoreRescalesKeyedIndex` now asserts that the pass path ran. Its restored savepoint is followed by
  the sink's own savepoint commit.
- The pass path was logged on 18 subtask restores across the existing failover, rescale and
  savepoint-after-compaction tests, all green.
- spotless and checkstyle are clean. Error Prone caught one boxed `Long ==` while writing this; it is fixed.

**Limits:**

- Snapshots that only remove data files (an external `DELETE FROM` that drops whole files) are not checked.
- With aggressive in-job expiry, a failover whose `lastSeenSnapshotId` was already expired fails with the
  "cannot verify" message, even though it is safe. No test hit this: the in-job maintenance tests keep 5
  snapshots.
- Modes A, B and C were not re-run, because the change is confined to the index-dv writer.

### Is skipping compaction snapshots safe? (commits `a8c5263d6`, `ec3a5b54e`)

The restore check skips `replace` snapshots. Cases c1–c6 in `TestIcebergSinkFailureScenarios` test whether that
can hide unseen rows.

**Setup.** Same deterministic input: 400 upserts over 60 keys. The external compaction:

- reads the live rows at the head;
- writes them into one file per partition;
- commits a `RewriteFiles`.

**Verdicts.** Each case reports one of three:

- OK: rows equal the model, no duplicate keys, no dangling DVs, no data file with two DVs;
- LOUD: the job failed;
- SILENT_WRONG: anything else.

The test asserts the expected verdict in D. B is the baseline.

| case | D, before the fix (`a8c5263d6`) | D, after the fix (`ec3a5b54e`) | B baseline |
|---|---|---|---|
| c1 full rewrite after the latest checkpoint, restore | OK (2 rewrites applied, 4 DVs rebased) | OK | OK |
| c2 partial rewrite (bucket 0 of `bucket(id,3)`), restore | OK | OK | OK |
| c3a stale restore, only a compaction after it | OK | OK | OK |
| c3b stale restore, sink commits, then a compaction rewriting them | LOUD (restore check) | LOUD (restore check) | OK |
| c3c stale restore, compaction, then sink commits | LOUD (restore check) | LOUD (restore check) | OK |
| c4 rewrite of files with live DVs (8), restore with no new input | OK, no resurrected rows | OK | OK |
| c5 as c3b, then expire every snapshot but the head | LOUD ("Cannot verify ... not an ancestor ... or its history was expired") | LOUD | OK |
| c5a as c1, then expire every snapshot but the head (files cleaned) | OK | OK | OK |
| c6 every live DV rewritten into a new DV file, restore | **LOUD, false failure**: `ValidationException: Found concurrently added DV` | **OK** | n/a (B has no DVs there) |

**The skip rule is safe in every case tried.** A compaction cannot hide rows from the check. The commits that
added those rows are themselves ancestors of the head, so the check meets them before it reaches the compaction
(c3b, c3c).

The only way to remove those commits is expiry. Expiry also cuts the parent chain, so the check fails with
"cannot verify" rather than passing (c5). When expiry keeps every snapshot after the restored one, the restore is
correct, and it passes (c5a).

c1, c2, c3a and c4 show the catch-up path working:

- records processed before the first barrier DV the removed files;
- the committer rebases those DVs by key;
- `applyRewrites()` then moves the index.

**c6 was a commit-path bug, not a check bug.** Two facts collide:

- `IndexDvCommits.build` merges each touched file's DV with that file's DV at the head;
- it validates from the sink's last snapshot.

A DV rewrite committed after that snapshot re-adds the DV. Core's `validateAddedDVs` then rejects every attempt,
even though the row delta replaces that DV and already includes its positions.

This also hits normal running, not only restores: any `rewrite-position-deletes` would put an index-dv job into
a failure loop. The fix (`ec3a5b54e`): when the commit replaces a live DV whose file sequence number is newer than
the sink's last snapshot, validate from the head, as the rebase path already does.

**Verification:**

- After the fix, the full failure suite passes 17 of 17 in D (s1–s6 unchanged) and 17 of 17 in B.
- `TestIcebergSinkIndexDv`: 84 run, 0 failures.

**Not covered:**

- a compaction running concurrently with the sink's commit (core validation decides; iteration 3);
- an external non-compaction writer during normal running, which `applyRewrites()` ignores (the known
  multi-writer gap, not restore-specific);
- Spark's `RewriteDataFiles` and `RewritePositionDeleteFiles` themselves. The tests use equivalent `RewriteFiles`
  commits in core.

### Rebuild on a stale restore (commit `4ee433707`)

**The option.** `upsert-stale-restore` = `fail` (default) | `rebuild`. It is a write option, named like `upsert-mode`
and `upsert-enabled` in `FlinkWriteOptions`.

**What a rebuild does.** It runs when the restore check finds the index behind the table:

- each writer subtask clears its keyed index entries and the file dictionary;
- it bootstraps from the branch head, applying the head's DVs;
- it logs a WARN with the restored snapshot, the head, the keys rebuilt and the time.

The DV writer also restarts from the head's DVs. That is needed after a rollback, where the last DV it remembers
for a file could hold positions the head considers live. It is safe even without a rebuild, because the committer
merges every DV with the file's DV at the head.

**Pending results.** A rebuild is refused when the restored state holds result files this sink wrote that the
table does not hold:

- Writers track such files from flush until a snapshot seen at a barrier adds them.
- At restore, the remaining ones are matched against files added in the window, files rewritten in the window, and
  files live at the head.
- Anything left is a result the restored committer will commit, with deletes resolved against the stale index.
  The rebuild fails loudly in that case.

**Why committed checkpoints are neither lost nor double-committed.** The committer skips every restored committable
at or below the `max-committed-checkpoint-id` of its job id and operator id in the head's history.

- In s2, s3 and s5b, the abandoned run or the sibling job committed later checkpoints under the same job id. So
  everything at or below the restored checkpoint was already committed and skipped, and the rebuild only re-indexes.
- p2 shows the opposite: a checkpoint whose commit failed is committed once by the restored committer when the
  table is not stale.
- p1 shows the dangerous mix (uncommitted results plus an external append): a rebuild is refused.

**Restored snapshot missing from the head's history** (c5: expired). The rebuilt index comes from the head alone,
so it does not need that history. Only the pending check does, and it also accepts files that are live at the head
or removed by a retained rewrite. That is why the c5 rebuild is safe.

**Results, mode D** (`fail` / `rebuild`; 31 cases each, all assertions pass):

| case | `fail` | `rebuild` |
|---|---|---|
| s2, s3, s5b (stale restore) | loud | correct; 2 subtasks rebuilt, 59–101 ms |
| c3b, c3c (stale + compaction) | loud | correct; 17–38 ms |
| c5 (stale + compaction + expired history) | loud ("cannot verify") | correct; 18 ms |
| p1 (failed commit of checkpoint N + external append, restore N) | loud | **loud: rebuild refused**, "holds 2 data file(s) this sink wrote that table ... does not hold" |
| p2 (failed commit of checkpoint N, restore N) | correct, N committed once | correct |
| s1, s4, s5a, s6a, s6b, c1–c6 | unchanged, correct | unchanged, correct, no rebuild |

**Rescale + rebuild.** `staleRestoreRebuildsIndexAtNewParallelism`: savepoint at parallelism 2, an external append
of a new key, restore at 4 with `rebuild`, then an upsert of that key. All 4 subtasks rebuilt (12+5+5+15 keys,
about 100 ms), and the rows match the model in every combination that runs.

**Rebuild time:**

| keys | fresh bootstrap | rebuild | rows after |
|---|---|---|---|
| 60 (scenarios) | – | 14–101 ms | model |
| 200k | 0.27 s | 0.30 s | 200 001 ✔ |
| 2M (2 runs) | 2.15 / 2.25 s | 2.47 / 2.73 s | 2 000 001 ✔ |

The rebuild costs a bootstrap plus clearing the old keys, 15–21% more at 2M (`IndexDvScaleBenchmark.staleRestoreRebuild`,
RocksDB, parallelism 2).

**Limits:**

- A head that already holds duplicate keys (written by another writer) stays duplicated: the rebuild indexes one
  row per key.
- A foreign commit that lands while the subtasks restore can let one subtask rebuild and another not. This is the
  multi-writer gap.
- Matching the committer's job and operator id exactly would need the aggregator's operator id in the writer; the
  pending check works from files instead.

### Compaction concurrent with sink commits (commits `29cd08b52`, `ccb3dac42`)

**Setup.** External `RewriteFiles` commits while the job runs, timed with test-only hooks in `IcebergCommitter`:

- one when a commit starts, after the committable is prepared and before the index-dv row delta is built;
- one right before each table commit attempt.

D and B, deterministic input, the same verdicts as above.

| case | D before `ccb3dac42` | D after | sink retries (D) | B |
|---|---|---|---|---|
| k1a full rewrite before the commit starts (×2) | correct | correct | 0 | correct |
| k1b partial (one bucket) before the commit starts | correct | correct | 0 | correct |
| k1c DV rewrite before the commit starts | correct | correct | 0 | correct |
| k2a full rewrite races the commit attempt (×2) | correct | correct | 2 | correct |
| k2b partial rewrite races the commit attempt | correct | correct | 2 | correct |
| k2c DV rewrite races the commit attempt | correct | correct | 2 | correct |
| k2d split rewrite (1 → 2 files) then full rewrite, both racing one commit | **SILENT_WRONG: 90 rows, 30 dup keys** | **correct** | 1 | correct |
| k3a a rewrite races the first attempt of every commit (7) | correct | correct | 7 | correct |
| k3b 6 rewrites in a row starve one commit (4 attempts), restarts allowed | correct after 1 restart | correct after 1 restart | 5 | correct |
| k4 external full rewrites every 500 ms on another thread | correct (some rewrites lose) | correct | 3 | correct, the rewrites lose |
| k4x external rewrites back to back, 3 restarts allowed (reported only) | **loud, unrecoverable**: "too many rewrites to rebase DV" | loud: starvation, `missing data files` | 12 = 4 × 4 runs | correct, 7 of 8 rewrites lose |
| k5 in-job rewrite on every commit + external every 500 ms | correct | correct | 3 | correct |

No case left dangling DVs or a data file with two DVs.

**Who wins.** In D, whoever commits first wins:

- a rewrite that lands before the sink commits makes the sink rebuild its row delta (rebase by key) and retry;
- a sink commit that lands first makes the rewrite fail its own validation (new deletes for replaced files).

B's eq-delete commits do not validate, so B's sink always wins and the compactor loses.

**Bug found, fixed in `ccb3dac42`.** `IndexDvCommits.rebase` followed only one rewritten-again output per hop.

- When a rewrite split a DV target's rows across several files and more than one of them was rewritten again
  before the commit, the other positions were dropped. Old rows stayed live next to their upserts, silently.
- k2d reproduced it deterministically: 30 duplicate keys.
- Any compaction that splits files by target size, then is followed by another compaction before the sink commits,
  hits it.
- The rebase now follows every such file.
- It also drops the cap of 8 rewrites per DV. With the cap, a pending commit became permanently uncommittable once
  more rewrites stacked up during a restart: every restart failed the same way (k4x before the fix).

**Remaining limit: starvation (k4x).** If rewrites of the same files land faster than one index-dv commit (about
160–300 ms here), the sink loses every attempt. The job fails after its retries and restarts, loudly, with no
corruption.

Realistic cadences are fine (k4, k5). The way out is to stop compaction from competing with the sink:

- run it in-job under the maintenance lock;
- have the planner skip files that the last commits wrote DVs to;
- or schedule it well apart from commits.

These need a design decision; they are not implemented.

**Regression after both commits:**

- the failure suite passes 31/31 in D `fail`, D `rebuild` and B;
- `TestIcebergSinkIndexDv`: 90 run, 0 failures, 41 skipped by design.

One assertion added in `195798a8b` turned out to be timing-dependent and was removed: a savepoint commit landing
after the restored head in `savepointRestoreRescalesKeyedIndex`.

**Not covered:**

- the Spark `RewriteDataFiles` and `RewritePositionDeleteFiles` actions themselves (equivalent core commits are
  used);
- a rewrite whose output file sizes are planned by the real planner;
- REST or Hive catalogs (HadoopCatalog only);
- more than one sink on one table.

## Part 2: resources (10M keys)

**Harness:** `IndexDvScaleBenchmark.resourceComparison`. Same setup as design note §4.4:

- a 10M-key table loaded first;
- then 1M random-key updates;
- then 1M clustered updates;
- parallelism 2, 1 s checkpoints, 8 MB target files, RocksDB incremental checkpoints, hash distribution.

Two rounds per mode, each on a fresh table. Cells show round 1 / round 2. CPU is process CPU time over the phase (`OperatingSystemMXBean`).
Busy and backpressure come from `InMemoryReporter`, taking the max and average over non-source tasks. Each
savepoint is taken once after the update phases, in canonical and native format.

| metric | A append | B eq deletes | C converter | D index-dv |
|---|---|---|---|---|
| bootstrap (index build) | n/a | n/a | inside the update phases (see below) | 11.5 / 10.9 s, CPU 29.6 / 27.2 s |
| 1M random updates | 0.95 / 0.95 s (1.05M/s) | 1.09 / 1.01 s (0.92–0.99M/s) | **96.6 / 96.9 s (10.4k / 10.3k/s)** | 6.1 / 6.4 s (164k / 156k/s) |
| CPU, random phase | 1.8 / 0.8 s | 4.9 / 2.6 s | **259.9 / 257.8 s** | 17.0 / 16.7 s |
| 1M clustered updates | 0.87 / 0.86 s | 0.57 / 0.94 s | **241.6 / 47.4 s (4.1k / 21.1k/s)** | 6.1 / 5.5 s (164k / 181k/s) |
| backpressure ms/s, max (avg) | 0 | 0 | **975–1000 (905–922)** random, 974–978 (162–727) clustered | 14 (1–13) |
| heap peak | 406–464 MB | 849–1243 MB | 1035–1149 MB | 421–541 MB |
| RocksDB block cache, max | n/a | n/a | 448 MB | 308–405 MB |
| RocksDB live data, max | n/a | n/a | 621 MB | 114–122 MB |
| checkpoints in run | 6 / 6 | 6 / 6 | 12 / 11 | 18 / 17 |
| checkpoint duration median / max | 22 / 6 ms, max 85–118 ms | 40 / 43 ms, max 150–340 ms | **283 / 311 ms, max 241.5 / 360.9 s** | 66 / 103 ms, max 5.2–5.8 s |
| checkpoint size incr. median / max | 0.7 KB / 3.4 KB | 0.7 KB / 6.6 KB | 139 KB / **744 MB** (both rounds) | 62–64 KB / 147 MB |
| full state size | 3.4 KB | 6.6 KB | **777–778 MB** | 147–150 MB |
| savepoint canonical | 6–13 ms, 2.3 KB | 9–16 ms, 2.3 KB | **96.8 / 243.9 s, 3.19 GB** | 2.1–2.3 s, 410 MB |
| savepoint native | 8–10 ms, 2.5 KB | 34–37 ms, 2.5 KB | 0.55–0.58 s, 626–627 MB | 0.35–0.45 s, 131–137 MB |
| wait for conversion to finish | n/a | n/a | 120.5 / 120.2 s | n/a |
| live eq delete refs / DV refs at end | 0 / 0 | **16** / 2 | 0 / 6 (both rounds) | 0 / 8–10 |
| `planFiles` | 12–32 ms | 16–33 ms | 16 ms | 20 ms |
| full scan, 10M rows | 1.0–1.1 s (12M rows, by design) | **2.8–3.1 s** | 1.09–1.13 s | 1.58–1.61 s |
| RSS | not measured | not measured | not measured | not measured |

C varies a lot between rounds: clustered 241.6 s vs 47.4 s, max checkpoint 241.5 s vs 360.9 s, canonical savepoint 96.8 s vs 243.9 s. In round 1 the 241 s checkpoint overlaps almost the whole clustered phase. The machine was close to its free-memory floor during both rounds (`vm_stat` free pages down to about 4k), so C's tail numbers are the least reliable in this table.

**Reading the numbers**

- **A and B write at about 1M rows/s**, bounded by the source, with no backpressure and tiny state. The cost of B
  shows up at read time:
  - the scan is 2.8–3.1 s versus 1.6 s for D and 1.1 s for A;
  - 16 eq delete files are applied to every data file;
  - the heap peak during the update phases is the highest, 0.85–1.24 GB (not profiled).
- **D costs about 6× more write time and 3.5–6.5× more CPU than B**, plus a one-off 11 s bootstrap per new job. In return:
  - no eq deletes;
  - a scan close to append;
  - a 147 MB keyed state, about 15 bytes per key on disk.
  - Its checkpoints are small and incremental (median 62 KB), but the first full-index checkpoint after bootstrap
    peaks at 147 MB / 5–6 s.
  - Backpressure stays at or below 14 ms/s.
- **C is the expensive mode at this scale:**
  - 16–40× slower than D on updates, with sustained backpressure (905 ms/s average during random updates);
  - 5× D's state (777 MB, a per-key position list plus a generation per key);
  - 241–361 s checkpoints, most likely while it builds its index from the 10M-row table (not profiled);
  - a canonical savepoint of 3.2 GB taking 97–244 s;
  - 2 more minutes after input stops to finish converting.
  - Once done, the table is clean (0 eq refs) and scans like D.
  - The B numbers show that the writer's eq deletes are cheap, so the cost is in the conversion. The likely
    driver is the converter building its index from the table at the first conversion; this was not profiled.
- **Native savepoints are 3–5× smaller and 5–175× faster than canonical** for the stateful modes (C, D). Canonical
  rewrites every RocksDB entry.
- **Busy time reads about 1000 ms/s in every mode**, so it does not discriminate here. Each phase is a burst that
  saturates whichever task is the bottleneck. Backpressure is the useful signal.

**C scale-down history.** For the record, this is why C took four attempts:

1. 10M: "Gradle build daemon disappeared unexpectedly" under memory pressure.
2. 10M: `OutOfMemoryError: Java heap space` in the test JVM.
3. 2M, 2 rounds: killed by the 30 min shell timeout.
4. 2M, 1 round: finished (job time 44 s, wall time 16 min).

The cause was log volume, not the job. `EqualityConvertPKIndex` logs `Index generation changed from null to 1 ...
clearing state` at INFO once per key, the first time it sees that key. The generation is keyed state, so every
new key logs. That was 2M lines (400 MB) at 2M keys, and the Gradle test-output pipe could not drain them. The
benchmark now sets that one logger to `warn` (slf4j-simple system property, test code only), after which C runs
at 10M.

- **Suggested upstream change:** log the generation change once per subtask, or at DEBUG.
- The 2M single-round result, with the logging still on, is kept only as a cross-check:
  - random 7.9k/s, clustered 39.7k/s;
  - checkpoint max 24.6 s;
  - state 232 MB;
  - canonical savepoint 640 MB in 1.6 s;
  - conversion wait 2.1 s.

## Not run, or not measured, and why

- **RSS.** `ps` is not permitted in the sandbox, and in-JVM there is no separate TaskManager process. Heap peak and
  RocksDB block cache, live data and memtable sizes are reported instead.
- **Real SIGKILL in the commit window (s2).** Not possible in-JVM. Replaced by cancel plus restore from an older
  retained checkpoint, which produces the same table and state relationship.
- **DeleteOrphanFiles and partial rewrites with index-dv (optional).** Not run. Per iteration 3, orphan cleanup
  against a lagging index is still fail-loud only.
- **No profiling.** The 6× D-versus-B write cost is not attributed. The likely costs are the RocksDB point lookup
  per record and the DV writer's serialization (iteration 3 notes).
- **Busy-time source exclusion.** The filter drops tasks whose name contains "Source". In a chained topology that
  drops the whole source chain, so "busy" reflects the remaining tasks only.

## Files

- `flink/v2.3/flink/src/test/java/org/apache/iceberg/flink/sink/TestIcebergSinkFailureScenarios.java` (new)
- `flink/v2.3/flink/src/test/java/org/apache/iceberg/flink/sink/IndexDvScaleBenchmark.java`
  (`resourceComparison`, reporter metrics, the converter logger set to warn)
- Run Part 1:

  ```
  FAILURE_MODE=<A|B|C|D> ./gradlew -DflinkVersions=2.3 -DsparkVersions= -DkafkaVersions= --max-workers=2 \
    --no-build-cache :iceberg-flink:iceberg-flink-2.3:test --rerun \
    --tests org.apache.iceberg.flink.sink.TestIcebergSinkFailureScenarios
  ```

- Run Part 2:

  ```
  INDEX_DV_RESOURCE_KEYS=10000000 INDEX_DV_RESOURCE_ROUNDS=2 INDEX_DV_RESOURCE_MODES=<A|B|C|D> ./gradlew ... \
    --tests "org.apache.iceberg.flink.sink.IndexDvScaleBenchmark.resourceComparison"
  ```

- `--rerun` is required. Environment variables are not Gradle task inputs, so without it a run can silently reuse
  another mode's result.

---
**AI Disclosure**
- Model: Claude (Anthropic)
- Platform/Tool: Kiro (Kiro Crew)
- Human Oversight: partially reviewed
- Prompt Summary: Implement the index-dv fail-fast restore check and an opt-in rebuild, test both against compactions and concurrent rewrites, fix what failed, and re-run D. Compare four Flink sink modes (append, eq-delete upsert, eq-delete + converter, index-dv) across
  six failure scenarios and on resources at 10M keys. The test harnesses, every measurement, the D root-cause
  analysis, the proposed fix, the fail-fast check and its tests, and this report are generated.
