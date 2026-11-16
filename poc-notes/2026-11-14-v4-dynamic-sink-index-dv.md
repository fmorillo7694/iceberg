# V4 Flink sink POC: index-dv mode for DynamicIcebergSink (local only, 2026-11-14)

Branch `poc-flink-index-dv-writer`, on top of `ccb3dac42`, not pushed. In-JVM MiniCluster, HadoopCatalog on local
disk, Flink 2.3, parallelism 2, laptop numbers. The design is §4.6 of
`reports/2026-11-12-v4-flink-sink-design-note.md`.

Commits:

- `f7a7c4351`: tolerate a missing spec when writing completed files to manifests. This is a regression in
  `FlinkManifestUtil` from iteration 1 (`4047cc628`), found by `TestDynamicWriteResultAggregator`.
- `d0b20fc90`: index-dv upsert mode for `DynamicIcebergSink`, with `TestDynamicIcebergSinkIndexDv`.
- `c6758e811`: `DynamicIndexDvBenchmark` (dynamic tables, key encodings).

## What the slice does

| piece | behaviour |
|---|---|
| routing | `upsert-mode=index-dv`: upsert records keyed by (table, branch, equality key); append records keep `DynamicWriter`. Per-table write parallelism and distribution mode are not honored (option A, §4.6 a) |
| index | `DynamicIndexDvDataWriter`: keyed `ValueState<Long>` (file id, pos); file dictionary and per-table metadata (last seen snapshot, equality ids, bootstrapped key-group ranges) in union state |
| lazy bootstrap | first record of a (table, branch) on a subtask: index the owned keys of key groups no subtask has indexed yet, from the branch head, applying its deletes |
| restore | per table, on its first record after the restore: fail if a non-rewrite snapshot after the restored view added a data file the dictionary lacks. Tables without records after the restore are not checked |
| DVs | `DynamicIndexDvDeleteWriter`, keyed by data file, one DV per file per checkpoint, split by spec. The committer merges with the file's live DV |
| results | `DynamicIndexDvRelaySink` through `SinkWriterOperatorFactory`, unioned into the existing aggregator like the forward writer |
| commit | `DynamicCommitter` in index mode commits each table and checkpoint through `IndexDvCommits` (rebase, liveness, retries). Sink-id dedup unchanged |
| limits (fail loudly) | equality fields must equal the table's identifier fields and must not change; V3+ tables; aligned checkpoints |

## Tests

`TestDynamicIcebergSinkIndexDv`: 8 tests. Each run uses 3 tables, interleaved upserts and deletes over 15–20 keys
per table, and duplicate keys within each checkpoint. Every test checks the rows against a per-table model, no
data file with more than one live DV, and zero equality delete files from index mode.

| test | result |
|---|---|
| 3 tables, upserts/deletes/dups, against the model and eq-delete mode | ✔ same rows as eq-delete mode; 0 eq deletes in index mode |
| a table appearing mid-stream (created on the fly) | ✔ no key indexed from the tables (all written by the sink) |
| existing tables (written by eq-delete mode) bootstrapped on first sight | ✔ keys indexed from the tables with their eq deletes and DVs applied |
| first record after a bootstrap uses its own key (parallelism 1, deterministic) | ✔ **regression test for a bug found here** |
| failover (restart from checkpoint) | ✔ |
| savepoint rescale 2 → 4 | ✔ no re-bootstrap (key groups already covered) |
| stale restore: external append to `t_b` | ✔ a restore writing only `t_a` passes; a restore writing `t_b` fails loudly naming `t_b` |
| schema change adding a non-key column mid-stream | ✔ keys unchanged, rows correct |

Three full-class runs in a row: 8/8 each.

**Regressions:**

- every `org.apache.iceberg.flink.sink.dynamic.*` test class passes, including `TestDynamicIcebergSink` 50/50 and
  `TestDynamicCommitter` 14/14 (after `f7a7c4351`);
- `TestIcebergSinkIndexDv`: 90 run, 0 failures, 41 skipped by design;
- spotless and checkstyle are clean.

**Bugs found while building it:**

1. **Lost DV positions after a lazy bootstrap (fixed before commit).** The bootstrap runs inside `processElement`
   and moves the keyed backend to each key it indexes. The record that triggered it then retired and stored under
   the last bootstrapped key:
   - that key's row was deleted;
   - the record's old row stayed live.

   It was intermittent with 3 tables, because later upserts often heal both keys. At parallelism 1 it is
   deterministic. The fix restores the record's key after activation.
2. **Per-key reordering from a filter at the wrong parallelism (fixed before commit).** Splitting upsert and append
   records with a `filter` on the union put a rebalance in front of the key-by, so records of one key reached it on
   two channels. The filter now runs on each input at that input's parallelism, so it chains.
   - The general rule: nothing between the generator and the key-by may reshuffle. The `Updater` path is the
     remaining exposure [verify].
3. **`FlinkManifestUtil` NPE with a null spec** (iteration-1 regression; `f7a7c4351`).

## Benchmark (rough, laptop)

**Setup.** RocksDB, incremental checkpoints, 1 s interval, parallelism 2. Each phase is 2 batches, each followed by
2 completed checkpoints.

**Dynamic, 3 tables × 1M keys:**

- 300k random updates spread over the tables, then 300k clustered;
- a warm-up of 64 keys per table first, which triggers the lazy bootstrap on both subtasks.

**Single-table reference:** `IndexDvScaleBenchmark.resourceComparison`, mode D, at 3M keys and the same update
volume.

| metric | dynamic, 3 × 1M (2 runs) | single table, 3M (IcebergSink, 2 runs) |
|---|---|---|
| bootstrap | 3M keys, 6 lazy table bootstraps; warm-up phase 4.3 / 4.5 s including its checkpoint waits | 2.8 / 2.9 s at startup |
| 300k random updates | 2.55 / 2.70 s (117k / 111k/s) | 1.58 / 1.24 s (190k / 242k/s) |
| 300k clustered updates | 3.30 / 3.15 s (91k / 95k/s) | 1.26 / 1.26 s (239k / 239k/s) |
| native savepoint | 38.0 MB (12.7 B/key), 0.12 / 0.63 s | 33.7 MB (11.2 B/key), 0.14 / 0.10 s |
| rows after | 1M per table ✔ | 3M ✔ |

- **Throughput:** these two columns come from different harnesses, which exaggerated the gap. On one harness it is
  about 15–20%, not 2×; see [Profile](#profile-of-the-update-gap).
- **State:** 13% larger, from the table name in every key.

## State size per key encoding (2M keys)

**Setup.** A keyed `ValueState<Long>` (the index's packed location) in RocksDB, measured as a native savepoint.
Native savepoints copy the SST files as they are, so compaction state adds noise of roughly ±30%.

| key encoding | raw key bytes | state bytes/key |
|---|---|---|
| int id, with table+branch prefix (the POC's dynamic key) | 39 | 13.4 |
| int id, no table prefix (single-table key) | 17 | 17.1 |
| long id, with prefix | 43 | 15.2 |
| UUID string, with prefix | 71 | 36.8 |
| 64-bit murmur3 hash of the UUID key | 8 | 22.3 |

- **The table-name prefix is close to free on disk.** RocksDB's prefix delta encoding and block compression absorb
  it. The int-with-prefix run was even smaller here, within the noise.
- **String keys cost about 2.7× an int key.**
- **A 64-bit hash saves about 40% against UUID keys**, but stays above an int key, because random hash bytes don't
  compress.

**Collision handling, if a hash ever replaces the key:**

1. **Accept the risk.** The chance of any collision is about n²/2⁶⁵: about 1e-7 at 2M keys, but about 3% at 1B.
   A collision silently merges two keys (a wrong DV, a lost row), so this is not acceptable at scale.
2. **Fingerprint.** Store the hash as the key and a second 32-bit fingerprint in the value. That cuts the risk by
   2³², and a mismatch is detected and can fall back to a full-key lookup.
3. **Verify on retire.** Before writing a DV, read the key at (file, pos) and compare. It is exact, but adds one
   random read per update. That is only viable with a positional cache.
4. **Collision chains.** Use `MapState<hash, List<(key, location)>>`, which stores the key again and gives back the
   saving.

Recommendation: keep full keys. Hashing is not worth it for numeric keys; for long string keys, option 2. The
profile below shows the table-name prefix is free on disk but not on CPU: it is most of the remaining update gap. A
small, stable table id per sink is the follow-up worth doing.

## Profile of the update gap

All on `IndexDvThroughputComparison` (commit `9ecdeeec0`). Every variant gets the same setup:

- one MiniCluster: RocksDB, incremental checkpoints, a 200 ms interval, parallelism 2, max parallelism 128;
- the same source and phases: a warm-up of 64 keys per table, then random and clustered updates. Each phase is 2
  batches, each followed by 2 completed checkpoints;
- one global key sequence, mapped onto the tables as `table = global % n`, `id = global / n`.

The variants:

- `single`: IcebergSink index-dv, one table with 3M keys;
- `single-prefix`: the same, with every key carrying the dynamic sink's `writeUTF(table) + writeUTF(branch)` prefix,
  through a test-only hook;
- `dynamic-1`: DynamicIcebergSink index-dv with one table of 3M keys;
- `dynamic-3`: DynamicIcebergSink index-dv with 3 tables × 1M keys.

Raw outputs, flame graphs and scripts are in `reports/2026-11-14-dynamic-index-dv-profile/`.

### Corrected baseline (unprofiled, updates/s)

| variant | 300k updates per phase, 2 rounds (random / clustered) | 1.5M updates per phase, 2 rounds (random / clustered) | mean at 1.5M |
|---|---|---|---|
| single | 152k / 145k, 192k / 209k | 263k / 248k, 277k / 232k | 255k |
| single-prefix | 148k / 149k, 164k / 191k | not run unprofiled | (−7% profiled) |
| dynamic-1 | 113k / 148k, 157k / 147k | 219k / 208k, 232k / 189k | 212k (−17%) |
| dynamic-3 | 99k / 77k, 105k / 121k | 223k / 192k, 214k / 190k | 205k (−20%) |

**The "2×" was mostly the harness.** The earlier comparison used two benchmarks with different checkpoint
intervals and phase timing.

- **On one harness**, the gap is about 15–20% at 1.5M updates per phase.
- **At 300k**, the gap is 35–50%, but those phases are 1–3 s long. Warm-up and the fixed checkpoint waits dominate
  them, and the noise between runs is ±10%.
- **dynamic-1 is about as slow as dynamic-3.** So the gap is not the number of tables. It is the dynamic code path
  and its key encoding.
- **The warm-up phase** is 4.4–4.8 s for the dynamic sink against 0.3 s for single. That is the lazy per-table
  bootstrap, and it is not part of update throughput.

### Profiles

Tool: async-profiler 4.1 (the official macOS release zip, unpacked into scratch, not installed). It was loaded
through `JAVA_TOOL_OPTIONS`, which keeps Gradle's `--add-opens`.

- Sampling: CPU by itimer every 2 ms, and allocations every 512 KB, output as JFR.
- Window: the random and clustered phases only, 3M updates.
- JFR alone was not usable: macOS `jdk.ExecutionSample` gave only 256 samples in 24 s.
- Profiling slows every variant by 10–25%, so the profiled throughput is not used for the gap.

**CPU-ms per 1M updates, by thread:**

| thread | single | single-prefix | dynamic-1 | dynamic-3 | dynamic-3, after fix |
|---|---|---|---|---|---|
| all | 9515 | 10217 | 12171 | 12574 | 12340 |
| index-dv writer (the bottleneck) | 6292 | 7115 | 7398 | 7559 | 7225 |
| delete writer | 786 | 706 | 885 | 1059 | 973 |
| source → generator → filters | 114 | 151 | 950 | 923 | 900 |
| JIT (C1/C2) | 1186 | 1047 | 1401 | 1612 | 1628 |

**Top 5 hotspots, as a share of all dynamic-3 CPU before the fix:**

1. **RocksDB put + delete: 23.1%.** The native copy inside the write batch alone is 14.7%. It is 1.7× the single
   path's copy cost per update, and single-prefix shows the same increase.
2. **Waking the delete writer for every retired position: 14.1%.** This is `Unsafe.unpark` →
   `__psynch_cvsignal` under `RecordWriter.emit` of each retirement. Single pays it too (17.9% of its CPU).
3. **RocksDB get: 13.0%.**
4. **Profiler allocation sampling: 12.9%.** This is a measurement artifact, but it grows with the allocation rate,
   so it inflates the dynamic profiles more.
5. **JIT compilation: 12.8%.** This is short-run warm-up, and similar on both paths.

The dynamic-only frames come after these: the generator at 6.8% (mostly off the bottleneck thread), the key
selector at 4.1% and `DynamicRecordInternalSerializer` at 3.5%.

**Allocation:** dynamic-3 allocates 6.9 GB per 1M updates against 2.7 GB for single. The largest dynamic-only
allocation sites were:

| site | MB per 1M updates |
|---|---|
| `DynamicIndexDvKeys.compose` | 594 |
| `DynamicIndexDvKeySelector.getKey`, the per-record cache-key string | 387 |
| `DynamicRecordInternalSerializer.copy` (the copying chain from generator to filters) | 271 |
| per-record `Namespace.of` / `TableIdentifier` in the generator and serializer | ≈110 |
| `DynamicRecordInternalSerializer.deserialize` | 120 |
| the per-record equality-field `HashSet` | 105 |

**Writer-thread breakdown (dynamic-3 minus single, CPU-ms per 1M):** about +1270 in total.

| source of the extra cost | before fix | after fix |
|---|---|---|
| RocksDB get/put/delete with the longer keys | +720 | |
| receive-side key selector (`setKeyContextElement`) | +250 (315 against 65) | 229 inclusive |
| `DynamicRecordInternal` deserialization | +143 (227 against 84) | unchanged |
| the rest, mostly profiler allocation sampling | small | |
| total over single-prefix | | +110 |

single-prefix alone adds +823 to the writer thread, and +782 of that is in RocksDB put/delete. So the dynamic
path's RocksDB cost is the key length, not the dynamic code. After the fix, dynamic-3's writer thread is only +110
over single-prefix.

### Fix (one commit)

**`a65931083`: no per-record strings in `DynamicIndexDvKeySelector`.** The extractor is now looked up by table
name, then schema id, with an equality-field check. The encoded (table, branch) prefix is reused, and keys are
composed with one array copy. The key bytes do not change.

| measure (dynamic-3) | before | after |
|---|---|---|
| key-selector CPU, ms per 1M | 513 | 229 |
| `compose` CPU, ms per 1M | 213 | 26 |
| key-selector allocation, MB per 1M | 1729 | 650 |
| total allocation, GB per 1M | 6.9 | 5.9 |
| writer-thread CPU | | −4.4% |
| unprofiled throughput, single | 255k | 240k |
| unprofiled throughput, dynamic-1 | 212k | 209k |
| unprofiled throughput, dynamic-3 | 205k | 207k |

**The throughput change is within run-to-run noise.** Single itself moved by 6% between those runs. The writer is
about 65–70% busy, and the profile shows the CPU saving. The tests pass:

- `TestDynamicIcebergSinkIndexDv`: 8/8;
- all 27 dynamic sink classes: 364 tests, 0 failures, 4 skipped;
- `TestIcebergSinkIndexDv`: 90 tests, 0 failures, 41 skipped as before.

### Not fixed

- **The table+branch prefix in every RocksDB key was most of the remaining gap.** It is about 18 bytes longer and
  cost about +800 CPU-ms per 1M updates on the writer. `79e04c82f` replaces it with an 8-byte table id; see
  [Table id instead of the table name](#table-id-instead-of-the-table-name-in-the-key).
- **`DynamicRecordInternalSerializer`** is about 2% of writer CPU, plus the generator's per-record
  `TableIdentifier` parsing and `toString`, plus the copy between the chained generator and filters (object reuse
  is off). This is upstream dynamic sink code, shared with every mode, so it is out of scope for the POC.
- **The per-retirement wake-up of the delete writer** costs about 14% of CPU on both paths. That makes it a
  single-path optimisation, not part of the gap. Retirements could be batched per checkpoint, or the delete writer
  chained.

### Unexplained

- **Why +18 key bytes almost doubles the native copy cost of RocksDB put/delete.** A plain copy would not do that.
  One hypothesis was libc++'s 22-byte inline-string limit. The table-id key does not fit it: at 25 raw bytes it is
  also above 22, yet its copy cost is back near single's. So the cost grows somewhere between the 25-byte id key
  and the 35-byte name key, for a reason the profile cannot show: the RocksDB JNI library has no symbols, and the
  frame names under the copy are approximate.
- **Off-CPU time.** The itimer profile counts only on-CPU time. The writer is not saturated, so part of each phase
  is hand-off and back-pressure latency, and the CPU profile cannot attribute it. A wall-clock profile would be next.
- **Resolution.** Run-to-run noise on this host is ±6–8%. Effects below about 5% of throughput cannot be resolved
  without more rounds.

## Table id instead of the table name in the key

Commit `79e04c82f`. The keyed-state key of the dynamic index-dv mode was `writeUTF(table) + writeUTF(branch)`
followed by the equality key. It is now an 8-byte table id followed by the same equality key.

### Id scheme

**Requirements.** The id must be identical on every subtask, and across restore, rescale, a savepoint into a new
job id, and restarts without state. It must never stand for two different (table, branch) pairs, including a table
that was dropped and re-created under the same name.

| option | how | verdict |
|---|---|---|
| (a) registry in union operator state | a subtask assigns the next id when it first sees a table, and ids are merged on restore | Rejected. Two subtasks can see a new table in the same checkpoint interval and pick different ids. The key-by runs before the writer, so the id would have to exist before the first record is keyed, which needs a round trip to the registry. Restarts without state renumber, which is fine, but the conflict handling is the hard part. |
| (b) **64-bit hash of (table UUID, branch)**, with collision detection | `murmur3_128(uuid.msb, uuid.lsb, branch)`, first 64 bits | **Chosen.** No coordination: any subtask, at any time, derives the same id. The UUID changes when a table is re-created and stays the same on a rename. |
| (c) OperatorCoordinator | the coordinator hands out ids | Rejected for the POC. It gives the same answer as (a) without the race, but needs a custom operator factory and an RPC on every new table, and it still has to persist the mapping. |

**Collision risk.** The chance that any two of n (table, branch) pairs collide is about n²/2⁶⁵. n counts every pair
the state ever held, because dropped tables are kept:

- 10³ pairs: 2.7·10⁻¹⁴;
- 10⁵ pairs: 2.7·10⁻¹⁰;
- 10⁶ pairs: 2.7·10⁻⁸.

**A collision fails loudly** before any keyed state of the second table is read or written. The per-table metadata
in union state records the UUID and branch behind each id. The writer compares them in three places:

- on restore, when two entries with the same id are merged;
- on a table's first record on a subtask, against the restored entry;
- on a record whose id is already active under another name or branch.

Two colliding tables can only share an index entry when one subtask receives records of both, and that subtask
then sees both ids. A 32-bit id would save 4 bytes, but at 10⁴ pairs the chance of a (loud) collision is about 1%.

**Key encoding:** a fixed 8-byte big-endian id. A varint does not apply, because hash ids are uniformly large.

### Where the id is computed

**A `DynamicIndexDvKeyAssigner` map, chained before the key-by** at each input's parallelism, resolves the table
UUID and attaches the whole key to the record as `Tuple2<key, record>`. The key selector returns that field.

- **Why not compute it in the key selector:** the receiving side's key selector would have to resolve the UUID
  again. If a table was re-created in between, the two sides would disagree on the key, and so on the key group.
  With the key attached, the writer uses exactly the key the record was partitioned by.
- **Cost:** it also removes the receive-side key computation, which was 229 CPU-ms per 1M updates.
- **UUID refresh:** the assigner re-reads the UUID from the catalog after `dynamic-sink.cache-refresh-ms`, the
  dynamic sink's existing cache interval (default 1 s).

### Rename, drop and re-create

**Rename.** The UUID is the same, so the id is the same and the index follows the table.

- **In the writer:** a record of a known id under a new name makes it reload the table by that name and check the
  UUID. It then writes, retires positions and emits results under the new name. Deletes are now routed by the
  table's current name, not by the name a data file was written under.
- **Records still addressed to the old name** either fail with no such table, or, if the table creator re-creates
  the table, get a new UUID and a new, empty index.
- **A commit pending under the old name** at the moment of the rename fails, because the dynamic committer commits
  by name. That is upstream behaviour for every mode.

**Drop and re-create.** The new table has a new UUID, so a new id and an index of its own, bootstrapped from its
own head. The old table's keys can never be looked up for the new table.

- **Records keyed with the old UUID after the switch**, within the refresh window, fail loudly instead of writing
  into the new table:
  - at the next barrier, the writer refreshes every table it wrote since the last one. A changed UUID, or Iceberg's
    own "Table UUID does not match" check, fails the job;
  - a record whose id does not match the table's current UUID fails on activation.
- **After a restart from the last checkpoint**, those records are keyed for the new table.

### Key format version and old state

The per-table metadata moved to a new union-state name, `dynamic-index-dv-tables-v2`. Each entry starts with the
key format version (2), followed by the id, UUID, branch and name.

**State in format 1** (the old `dynamic-index-dv-tables` list) is detected per table, on its first record:

- **by default, it fails loudly:** "uses key format 1 … Start the job without state, or set
  upsert-stale-restore=rebuild";
- **with `upsert-stale-restore=rebuild`**, the table is indexed from its head as if it had no state. Format 1 keys
  can never match format 2 keys, so they are simply never read again.
  - The rebuild is refused while the restored file dictionary knows data files of the table that its head does
    not hold: the restored committer may still commit them over the rebuilt index.
  - shortcut: the format 1 keys stay in the keyed state.

There is no migration.

### Tests

The new tests are in `TestDynamicIcebergSinkIndexDv` (14 tests, 14 pass).

| test | checks | result |
|---|---|---|
| the 8 existing tests (failover, savepoint restore with rescale 2 → 4, stale restore, …) | unchanged behaviour on format 2; the rescale test restores a format 2 savepoint into a new job, at 4 subtasks, with 0 keys bootstrapped | pass |
| `sameTableNameInTwoNamespacesKeepsSeparateIndexes` | `default.t_a` and `other.t_a` with interleaved upserts match the model | pass |
| `tableRecreatedMidRunStartsAFreshIndex` | drop and re-create `t_a` while the job runs. Afterwards the table holds exactly the new rows, and no DV references a data file of the old table | pass |
| `renamedTableKeepsItsIndex` | JDBC (sqlite) catalog, since `HadoopCatalog` cannot rename. After `r_a` → `r_b`, upserts dedupe against pre-rename rows, and nothing is bootstrapped | pass |
| `keyFormatOneRestoreFailsLoudly` | restore of format 1 per-table metadata fails with the format message | pass |
| `keyFormatOneRestoreRebuildsFromTheHead` | the same with `upsert-stale-restore=rebuild` matches the model, with rebuilds and bootstrapped keys counted | pass |
| `tableIdCollisionFailsLoudly` | a test hook maps every table to id 42; two tables fail with "Table id collision" naming both | pass |

**Test hooks and limits:**
- **Format 1 state is produced by a test hook** that writes the per-table metadata in format 1 and keys records with
  another id function. Real format 1 keys carry the table name, but either way they never match format 2 keys.
- **The drop and rename tests wait for the commits of the batch before the change to land.** A change during a
  commit fails that commit by name, which is the upstream race described above.

**Suites:**
- all 27 dynamic sink classes: 370 tests, 0 failures, 4 skipped;
- `TestIcebergSinkIndexDv`: 90 tests, 0 failures, 41 skipped as before;
- spotless and checkstyle clean.

### Effect

**Throughput.** Unprofiled, 3M keys, 1.5M updates per phase, 2 rounds, updates/s; each cell is random/clustered for
round 1, then round 2. The name-prefix column is `a65931083`, checked out and run in the same session.

| variant | name prefix (`a65931083`) | table id (`79e04c82f`) |
|---|---|---|
| single | 257k / 232k, 259k / 249k (mean 249k) | 233k / 253k, 258k / 241k (mean 246k) |
| single-prefix (control) | n/a | 252k / 205k, 252k / 196k (mean 226k) |
| dynamic-1 | 219k / 214k, 221k / 208k (mean 216k, **−14%** vs single) | 237k / 230k, 243k / 236k (mean 237k, **−4%**) |
| dynamic-3 | 205k / 203k, 191k / 195k (mean 199k, **−20%**) | 231k / 206k, 209k / 219k (mean 216k, **−12%**) |

- dynamic-1 gained about 10% and dynamic-3 about 9%, larger than the noise between runs (±6–8%) for both.
- dynamic-1 is now within noise of single.
- dynamic-3 keeps about −12%. That is the cost of `DynamicRecordInternal` deserialization (`TupleSerializer`, 254
  CPU-ms per 1M updates on the writer) and the three-table generator path.

**Writer-thread CPU, CPU-ms per 1M updates, one profiled run each:**

| | single | single-prefix | dynamic-3, name | dynamic-3, id |
|---|---|---|---|---|
| index-dv writer, total | 6292 | 7115 | 7225 | **6757** |
| RocksDB put + delete | 2337 | 3119 | 2923 | **2285** |
| of which the native copy | 1079 | 1988 | 1872 | **1202** |
| receive-side key selector | 65 | 93 | 229 | ≈0 (not sampled) |

The source thread adds about 220 CPU-ms per 1M for the key assigner and the key on the wire. That thread is not the
bottleneck.

**State bytes per key at 2M keys** (`DynamicIndexDvBenchmark.keyEncodingStateSize`, native savepoint, ±30% from
compaction state):

| encoding | raw key bytes | state bytes per key |
|---|---|---|
| int key with name prefix (format 1) | 39 | 13.4 |
| int key with table id (format 2) | 25 | 17.2 |
| int key, no table part | 17 | 17.1 |

- **The 8-byte id costs nothing on disk** compared with no table part at all.
- **The name-prefixed run is smaller in both runs so far.** That is a compaction artefact of the measurement, not
  something the longer key buys.
- **On CPU, the id is what matters.** With longer real table names, the name-prefixed key grows while the id stays
  8 bytes.

## Compaction on the dynamic path

**Setup.** The single-table compaction cases are ported in `TestDynamicIcebergSinkIndexDvCompaction` (`9a4858de1`):

- three tables (`t_a`, `t_b`, `t_c`) in one stream, 200 upserts each over 40 keys;
- every external rewrite targets `t_a` only, while the other two keep writing;
- rewrites are timed with test-only hooks in `DynamicCommitter`, fired for `t_a` only: when its commit is requested
  (before the index-dv row delta is built), and right before each of its commit attempts;
- `DYN_COMPACTION_MODE=equality-delete` runs the same cases as the baseline.

**Checks per table:**

- rows equal the model, with no duplicate keys, lost rows or unexpected rows;
- no DV on a data file that is no longer live;
- no data file with two live DVs.

`t_b` and `t_c` must also have no replace snapshot. Verdicts are OK, LOUD (the job failed) or SILENT_WRONG.

| case | index-dv | sink retries | eq-delete baseline |
|---|---|---|---|
| c1 full rewrite after the latest checkpoint, restore | OK | 0 | OK |
| c2 partial rewrite (bucket 0 of `bucket(id,3)`), restore | OK | 0 | OK |
| c3a stale restore, only a compaction after it | OK | 0 | OK |
| c3b stale restore, sink commits, then a compaction | LOUD (restore check) | 0 | OK |
| c3c stale restore, compaction, then sink commits | LOUD (restore check) | 0 | OK |
| c4 rewrite of files with live DVs, restore with no new input | OK, no rows come back | 0 | OK |
| c5 as c3b, then expire all but the head | LOUD ("cannot verify") | 0 | OK |
| c5a as c1, then expire all but the head | **LOUD before `006177e5b`** (false failure), OK after | 0 | OK |
| c6 every live DV rewritten into a new DV file, restore | OK | 0 | n/a (no DVs) |
| k1a/b/c full / partial / DV rewrite before the commit starts (×2) | OK | 0 | OK |
| k2a/b full / partial rewrite races the commit attempt (×2) | OK | 2 | OK |
| k2c DV rewrite races the commit attempt (×2) | OK | 0 | n/a |
| k2d split rewrite (1 → 2 files), then a full rewrite, racing one commit | OK | 1 | OK |
| k3a a rewrite races the first attempt of every `t_a` commit | OK, 8 rewrites | 7 | OK, 8 rewrites |
| k3b six rewrites in a row starve one commit, 2 restarts allowed | OK after a restart | 5 | OK |
| k4 background full rewrites every 500 ms | OK, 6 rewrites won and 1 lost to the sink | 2 | OK |
| k4x back-to-back rewrites, 3 restarts allowed (report only) | LOUD: `missing data files`, 117 rewrites | 12 | OK, 24 of 256 rewrites win |

- **Notes on the table:**
  - The stale cases fail on whichever stale table is checked first: all three tables committed after the
    restored checkpoint.
  - The few rewrite failures not counted as losses are no-ops ("files to delete cannot be empty", before the
    first commit).
  - The baseline needs no retries: its eq-delete commits do not validate, so the sink always wins.
- **Who wins** in index-dv is whoever commits first, as on `IcebergSink`:
  - a rewrite first makes the sink rebase its DVs by key and retry;
  - a sink commit first makes the rewrite fail its own validation ("new delete for replaced data file").
- **Untargeted tables were unaffected in every case:** their rows matched the model, with no replace snapshot and
  no stray DV.
- **Repeats:** the index-dv cases ran 3 times (2 after the fix) and the baseline once. The verdicts did not change
  between runs; the retry counts moved by at most 1.

**Bug found, fixed in `006177e5b`.** It was a loud false failure; nothing silently wrong turned up.

- **The cause:** the dynamic writer refreshed a table at a barrier only when it had written to it since the last
  barrier. Once the table went idle, its last seen snapshot stayed one commit behind the sink's own last commit.
- **What it broke:** if a compaction and then an expiry removed that commit, a correct restore failed with "cannot
  verify" (c5a). `IcebergSink` refreshes at every barrier and passed the same case.
- **The fix:** a table that wrote data in checkpoint N is refreshed at each barrier until its head holds this job's
  commit of N. That means the snapshot summary has the job id and `max-committed-checkpoint-id` ≥ N.
  - It costs one table refresh per barrier for a few intervals after a table's last write, not one for every idle
    table.
  - shortcut: a checkpoint that commits nothing for the table keeps it refreshed until its next write.

**Suites after the fix:**

- all 28 dynamic sink classes: 390 tests, 0 failures, 4 skipped, including `TestDynamicIcebergSinkIndexDv` 14/14;
- `TestIcebergSinkIndexDv`: 90 tests, 0 failures, 41 skipped;
- the single-table `TestIcebergSinkFailureScenarios`: 31/31, with 24 OK and 7 LOUD as before;
- spotless and checkstyle clean.

## Not covered

- **Operations on the dynamic path:**
  - the `upsert-stale-restore=rebuild` option for a stale format 2 index (rebuild per table needs a per-table key
    iteration); a format 1 index is rebuilt;
  - equality-field changes (fail loudly);
  - cleanup of dropped tables: the index entries of an old table id stay in the keyed state, and its per-table
    metadata stays in union state;
  - in-job maintenance. The dynamic sink has none, so compaction for these tables stays an open item (§4.6 h).
- **Concurrency:**
  - ordering of records that take the `Updater` path (`immediateTableUpdate=false`); the tests use `true`;
  - concurrent writers or external compaction on dynamic tables;
  - in-job compaction on the dynamic path (k5): the dynamic sink has no post-commit maintenance;
  - `upsert-stale-restore=rebuild` for a stale format 2 index, so stale-restore cases fail loudly with either
    setting.
- **Routing and layout:** a per-table branch other than `main`, partitioned tables, and per-table write parallelism
  (option B).
- **Scale:** many tables (hundreds), and memory budgets for open writers and the dictionary.

---
**AI Disclosure**
- Model: Claude (Anthropic)
- Platform/Tool: Kiro (Kiro Crew)
- Human Oversight: partially reviewed
- Prompt Summary: Design index-dv mode for DynamicIcebergSink (design note §4.6), implement a minimal slice with
  tests and a rough benchmark, and measure state size per key encoding. The design, code, tests, measurements and
  this report are generated.
