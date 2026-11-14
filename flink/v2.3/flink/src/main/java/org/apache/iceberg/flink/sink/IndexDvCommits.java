/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructProjection;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC: commit-side support for the index-dv upsert mode.
 *
 * <p>The writers compute deletion vectors against their view of the table. Before a commit, this
 * class reconciles them with the current branch head: a DV for a data file that a rewrite removed
 * is re-resolved by key to the rows the rewrite carried into its new files, and every DV is merged
 * with the live DV of its data file. A commit that still fails validation is rebuilt against the
 * new head a bounded number of times.
 */
// POC: public so that the dynamic sink's committer can use it.
@Internal
public class IndexDvCommits {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDvCommits.class);
  private static final int MAX_ATTEMPTS = 4;

  @VisibleForTesting static final AtomicLong REBASED_DVS = new AtomicLong();
  @VisibleForTesting static final AtomicLong RETRIES = new AtomicLong();

  private IndexDvCommits() {}

  // POC: test-only counters, public for the dynamic sink's tests in another package.
  public static long retries() {
    return RETRIES.get();
  }

  public static long rebasedDvs() {
    return REBASED_DVS.get();
  }

  public static void resetCounters() {
    RETRIES.set(0);
    REBASED_DVS.set(0);
  }

  /** Commits the files of one checkpoint, rebuilding the row delta on validation failures. */
  public static void commit(
      Table table,
      String branch,
      WriteResult result,
      String operatorId,
      Long bootstrapSnapshotId,
      Set<Integer> equalityFieldIds,
      Consumer<RowDelta> commitOperation) {
    for (int attempt = 1; ; attempt++) {
      RowDelta rowDelta =
          build(table, branch, result, operatorId, bootstrapSnapshotId, equalityFieldIds);
      try {
        commitOperation.accept(rowDelta);
        return;
      } catch (ValidationException e) {
        if (attempt >= MAX_ATTEMPTS) {
          throw e;
        }

        RETRIES.incrementAndGet();
        LOG.warn("Index-dv commit attempt {} failed validation, rebuilding", attempt, e);
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static RowDelta build(
      Table table,
      String branch,
      WriteResult result,
      String operatorId,
      Long bootstrapSnapshotId,
      Set<Integer> equalityFieldIds) {
    for (DeleteFile deleteFile : result.deleteFiles()) {
      Preconditions.checkState(
          ContentFileUtil.isDV(deleteFile),
          "Upsert mode 'index-dv' must commit only deletion vectors, found %s",
          deleteFile.location());
    }

    table.refresh();
    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : result.dataFiles()) {
      rowDelta.addRows(dataFile);
    }

    Snapshot head = table.snapshot(branch);
    if (head == null) {
      for (DeleteFile dv : result.deleteFiles()) {
        rowDelta.addDeletes(dv);
      }

      return rowDelta;
    }

    long start = validationStart(table, head, operatorId, bootstrapSnapshotId);
    Set<String> referenced = Sets.newHashSet();
    for (DeleteFile dv : result.deleteFiles()) {
      referenced.add(dv.referencedDataFile());
    }

    // Liveness comes from the head, not from a window: the index may still point at a data file
    // that a rewrite removed before this sink's last commit.
    Set<String> liveFiles = liveDataFiles(table, head, referenced);
    for (DataFile dataFile : result.dataFiles()) {
      // A key written twice in one checkpoint deletes from a data file added by this same commit.
      liveFiles.add(dataFile.location());
    }
    Removals removed = new Removals(table, head, liveFiles);
    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file));

    // Positions to delete per live data file, for the DVs that must be (re)written here.
    Map<String, Target> rewrites = Maps.newHashMap();
    List<DeleteFile> keep = Lists.newArrayList();
    for (DeleteFile dv : result.deleteFiles()) {
      String path = dv.referencedDataFile();
      if (!liveFiles.contains(path)) {
        Roaring64Bitmap positions = bitmap(loader.loadPositionDeletes(ImmutableList.of(dv), path));
        rebase(table, path, positions, removed, equalityFieldIds, loader, rewrites);
        REBASED_DVS.incrementAndGet();
      } else {
        keep.add(dv);
      }
    }

    Set<String> touched = Sets.newHashSet(rewrites.keySet());
    keep.forEach(dv -> touched.add(dv.referencedDataFile()));
    Map<String, DeleteFile> live = liveDvs(table, head, touched);
    long startSequence = table.snapshot(start).sequenceNumber();
    boolean replacesNewerDv = false;

    for (DeleteFile dv : keep) {
      String path = dv.referencedDataFile();
      DeleteFile liveDv = live.get(path);
      if (liveDv == null && !rewrites.containsKey(path)) {
        rowDelta.addDeletes(dv);
        continue;
      }

      Roaring64Bitmap ours = bitmap(loader.loadPositionDeletes(ImmutableList.of(dv), path));
      if (!rewrites.containsKey(path)
          && contains(ours, bitmap(loader.loadPositionDeletes(ImmutableList.of(liveDv), path)))) {
        rowDelta.addDeletes(dv);
        rowDelta.removeDeletes(liveDv);
        replacesNewerDv |= addedAfter(liveDv, startSequence);
      } else {
        // The writer did not see the live DV, for example one written by a rebase: merge them.
        rewrites
            .computeIfAbsent(path, p -> new Target(dv.specId(), dv.partition()))
            .positions
            .or(ours);
      }
    }

    for (Map.Entry<String, Target> entry : rewrites.entrySet()) {
      DeleteFile liveDv = live.get(entry.getKey());
      if (liveDv != null) {
        entry
            .getValue()
            .positions
            .or(bitmap(loader.loadPositionDeletes(ImmutableList.of(liveDv), entry.getKey())));
        rowDelta.removeDeletes(liveDv);
        replacesNewerDv |= addedAfter(liveDv, startSequence);
      }
    }

    for (DeleteFile dv : writeDvs(table, rewrites)) {
      rowDelta.addDeletes(dv);
    }

    // After a rebase, or when a DV committed since start (a DV rewrite) is replaced, every DV is
    // computed against the head, so validate from there.
    rowDelta.validateFromSnapshot(removed.rebased() || replacesNewerDv ? head.snapshotId() : start);
    rowDelta.validateDataFilesExist(touched);
    rowDelta.validateDeletedFiles();
    rowDelta.validateNoConflictingDeleteFiles();
    return rowDelta;
  }

  /**
   * Re-resolves positions of a data file that a rewrite removed: reads the keys at those positions
   * and finds the rows the rewrite carried into its added files, following later rewrites too.
   */
  private static void rebase(
      Table table,
      String path,
      Roaring64Bitmap positions,
      Removals removed,
      Set<Integer> equalityFieldIds,
      DeleteLoader loader,
      Map<String, Target> rewrites) {
    Schema keySchema = TypeUtil.select(table.schema(), equalityFieldIds);
    // Each queued file was added by the rewrite that removed the one before it, so its own removal
    // is later in the finite history: the walk ends.
    Deque<Pair<String, Roaring64Bitmap>> pending = new ArrayDeque<>();
    pending.add(Pair.of(path, positions));
    while (!pending.isEmpty()) {
      Pair<String, Roaring64Bitmap> next = pending.poll();
      String currentPath = next.first();
      Roaring64Bitmap current = next.second();
      Pair<Snapshot, DataFile> removal = removed.of(currentPath);
      Snapshot rewrite = removal.first();
      Preconditions.checkState(
          DataOperations.REPLACE.equals(rewrite.operation()),
          "POC: index-dv can only rebase over rewrites, data file %s was removed by %s (%s)",
          currentPath,
          rewrite.snapshotId(),
          rewrite.operation());

      // Rows already deleted when the rewrite ran were not carried over and need no rebase.
      Roaring64Bitmap wanted = current.clone();
      wanted.andNot(deletedAt(table, rewrite.parentId(), currentPath, loader));
      if (wanted.isEmpty()) {
        continue;
      }

      Set<SerializedEqualityValues> keys = Sets.newHashSet();
      readKeys(
          table,
          removal.second(),
          keySchema,
          (key, pos) -> {
            if (wanted.contains(pos)) {
              keys.add(key);
            }
          });

      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(rewrite).build();
      for (DataFile added : changes.addedDataFiles()) {
        Roaring64Bitmap found = new Roaring64Bitmap();
        readKeys(
            table,
            added,
            keySchema,
            (key, pos) -> {
              if (keys.contains(key)) {
                found.addLong(pos);
              }
            });
        if (found.isEmpty()) {
          continue;
        }

        if (!removed.isLive(added.location())) {
          pending.add(Pair.of(added.location(), found));
        } else {
          rewrites
              .computeIfAbsent(added.location(), p -> new Target(added.specId(), added.partition()))
              .positions
              .or(found);
        }
      }
    }
  }

  private static Roaring64Bitmap deletedAt(
      Table table, Long snapshotId, String path, DeleteLoader loader) {
    if (snapshotId == null) {
      return new Roaring64Bitmap();
    }

    DeleteFile dv = liveDvs(table, table.snapshot(snapshotId), Sets.newHashSet(path)).get(path);
    return dv == null
        ? new Roaring64Bitmap()
        : bitmap(loader.loadPositionDeletes(ImmutableList.of(dv), path));
  }

  private static Roaring64Bitmap bitmap(PositionDeleteIndex index) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    index.forEach(bitmap::addLong);
    return bitmap;
  }

  @FunctionalInterface
  private interface KeyAtPosition {
    void accept(SerializedEqualityValues key, long pos);
  }

  private static void readKeys(
      Table table, DataFile file, Schema keySchema, KeyAtPosition consumer) {
    List<NestedField> columns = Lists.newArrayList(keySchema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    Schema readSchema = new Schema(columns);
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keySchema.asStruct());
    StructLikeSerializer serializer = new StructLikeSerializer();
    Accessor<StructLike> posAccessor =
        readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    StructProjection projection = StructProjection.create(readSchema, keySchema);
    try (CloseableIterable<Record> records =
        FormatModelRegistry.<Record, Schema>readBuilder(
                file.format(), Record.class, table.io().newInputFile(file.location()))
            .project(readSchema)
            .build()) {
      for (Record record : records) {
        consumer.accept(
            serializer.serializeKey(keyWrapper.wrap(projection.wrap(record)), keySchema.asStruct()),
            (long) posAccessor.get(record));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean contains(Roaring64Bitmap outer, Roaring64Bitmap inner) {
    Roaring64Bitmap missing = inner.clone();
    missing.andNot(outer);
    return missing.isEmpty();
  }

  private static List<DeleteFile> writeDvs(Table table, Map<String, Target> targets) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build();
    BaseDVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (writer) {
      for (Map.Entry<String, Target> entry : targets.entrySet()) {
        PartitionSpec spec = table.specs().get(entry.getValue().specId);
        StructLike partition = entry.getValue().partition;
        entry
            .getValue()
            .positions
            .forEach(pos -> writer.delete(entry.getKey(), pos, spec, partition));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    DeleteWriteResult result = writer.result();
    return result.deleteFiles();
  }

  /** The referenced data files that are live in the snapshot. */
  private static Set<String> liveDataFiles(Table table, Snapshot snapshot, Set<String> paths) {
    Set<String> live = Sets.newHashSet();
    if (paths.isEmpty()) {
      return live;
    }

    // shortcut: reads every data manifest of the head, upgrade to a file-path lookup
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          if (paths.contains(file.location())) {
            live.add(file.location());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return live;
  }

  /** Finds, on demand, the snapshot that removed a data file and whether a file is still live. */
  private static final class Removals {
    private final Table table;
    private final Snapshot head;
    private final Set<String> knownLive;
    private final Map<String, Pair<Snapshot, DataFile>> found = Maps.newHashMap();
    private Snapshot nextToScan;
    private boolean rebased = false;

    private Removals(Table table, Snapshot head, Set<String> knownLive) {
      this.table = table;
      this.head = head;
      this.knownLive = knownLive;
      this.nextToScan = head;
    }

    boolean rebased() {
      return rebased;
    }

    boolean isLive(String path) {
      return knownLive.contains(path)
          || !liveDataFiles(table, head, Sets.newHashSet(path)).isEmpty();
    }

    Pair<Snapshot, DataFile> of(String path) {
      this.rebased = true;
      while (!found.containsKey(path) && nextToScan != null) {
        if (!DataOperations.APPEND.equals(nextToScan.operation())) {
          SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(nextToScan).build();
          for (DataFile file : changes.removedDataFiles()) {
            found.putIfAbsent(file.location(), Pair.of(nextToScan, file.copy()));
          }
        }

        nextToScan = nextToScan.parentId() == null ? null : table.snapshot(nextToScan.parentId());
      }

      Pair<Snapshot, DataFile> removal = found.get(path);
      Preconditions.checkState(
          removal != null,
          "POC: data file %s is not live and its removal is no longer in the table history; "
              + "an index-dv DV for it can't be rebased (were snapshots expired too early?)",
          path);
      return removal;
    }
  }

  private static Map<String, DeleteFile> liveDvs(
      Table table, Snapshot snapshot, Set<String> paths) {
    Map<String, DeleteFile> live = Maps.newHashMap();
    if (snapshot == null || paths.isEmpty()) {
      return live;
    }

    // shortcut: reads every delete manifest of the snapshot, upgrade to a DV lookup
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (ContentFileUtil.isDV(file) && paths.contains(file.referencedDataFile())) {
            live.put(file.referencedDataFile(), file.copy());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return live;
  }

  private static boolean addedAfter(DeleteFile dv, long sequenceNumber) {
    return dv.fileSequenceNumber() != null && dv.fileSequenceNumber() > sequenceNumber;
  }

  private static long validationStart(
      Table table, Snapshot head, String operatorId, Long bootstrapSnapshotId) {
    for (Snapshot snapshot = head;
        snapshot != null;
        snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId())) {
      if (operatorId.equals(snapshot.summary().get(SinkUtil.OPERATOR_ID))) {
        return snapshot.snapshotId();
      }
    }

    if (bootstrapSnapshotId != null
        && SnapshotUtil.isAncestorOf(table, head.snapshotId(), bootstrapSnapshotId)) {
      return bootstrapSnapshotId;
    }

    return head.snapshotId();
  }

  private static final class Target {
    private final int specId;
    private final StructLike partition;
    private final Roaring64Bitmap positions = new Roaring64Bitmap();

    private Target(int specId, StructLike partition) {
      this.specId = specId;
      this.partition = partition;
    }
  }
}
