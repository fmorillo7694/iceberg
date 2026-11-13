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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
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
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC: upsert writer keyed by the equality key. It writes the data files itself, so the position of
 * every new row is known when it is written, and keeps the location of each key's live row in keyed
 * state. A previous row is retired by emitting its position to {@link #DELETES}, which a DV writer
 * keyed by data file turns into deletion vectors.
 */
class IndexDvDataWriter extends AbstractStreamOperator<FlinkWriteResult>
    implements OneInputStreamOperator<RowData, FlinkWriteResult>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDvDataWriter.class);

  static final OutputTag<DVPosition> DELETES =
      new OutputTag<>("index-dv-deletes", TypeInformation.of(DVPosition.class));

  private static final int SUBTASK_BITS = 12;
  private static final long MAX_POSITION = 0xFFFFFFFFL;

  @VisibleForTesting static final AtomicLong LAST_BOOTSTRAP_MILLIS = new AtomicLong(-1);
  @VisibleForTesting static final AtomicLong BOOTSTRAPPED_KEYS = new AtomicLong();
  @VisibleForTesting static final AtomicLong REWRITES_APPLIED = new AtomicLong();
  @VisibleForTesting static final AtomicLong RESTORES_PAST_HEAD = new AtomicLong();
  @VisibleForTesting static final AtomicLong REBUILDS = new AtomicLong();
  @VisibleForTesting static final AtomicLong LAST_REBUILD_MILLIS = new AtomicLong(-1);

  private static final ValueStateDescriptor<Long> LOCATION =
      new ValueStateDescriptor<>("index-dv-location", Types.LONG);

  private final TableLoader tableLoader;
  private final String branch;
  private final RowType flinkRowType;
  private final Set<Integer> equalityFieldIds;
  private final FileFormat format;
  private final long targetFileSize;
  private final Map<String, String> writeProperties;
  private final Long bootstrapSnapshotId;
  private final boolean rebuildOnStaleRestore;

  private transient Table table;
  private transient PartitionSpec spec;
  private transient FileWriterFactory<RowData> fileWriterFactory;
  private transient OutputFileFactory fileFactory;
  private transient RowDataWrapper wrapper;
  private transient PartitionKey partitionKey;
  private transient StructLikeSerializer serializer;
  private transient Map<StructLike, RollingDataWriter<RowData>> dataWriters;

  private transient ValueState<Long> location;
  private transient ListState<byte[]> fileDictionaryState;
  private transient ListState<Long> lastSeenSnapshotState;
  private transient Long lastSeenSnapshotId;
  // Data files this subtask wrote that no snapshot seen at a barrier has added yet; kept for a
  // rebuild only.
  private transient ListState<String> unconfirmedState;
  private transient Set<String> unconfirmed;
  private transient Map<Integer, FileEntry> files;
  private transient Map<String, Integer> fileIds;
  private transient int subtaskIndex;
  private transient int nextFileCounter;
  private transient boolean inputEnded;

  static final class FileEntry {
    private final int id;
    private final String path;
    private final int specId;
    private final byte[] partition;

    FileEntry(int id, String path, int specId, byte[] partition) {
      this.id = id;
      this.path = path;
      this.specId = specId;
      this.partition = partition;
    }

    int id() {
      return id;
    }

    String path() {
      return path;
    }

    int specId() {
      return specId;
    }

    byte[] partition() {
      return partition;
    }
  }

  IndexDvDataWriter(
      TableLoader tableLoader,
      String branch,
      RowType flinkRowType,
      Set<Integer> equalityFieldIds,
      FileFormat format,
      long targetFileSize,
      Map<String, String> writeProperties,
      Long bootstrapSnapshotId,
      boolean rebuildOnStaleRestore) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.flinkRowType = flinkRowType;
    this.equalityFieldIds = equalityFieldIds;
    this.format = format;
    this.targetFileSize = targetFileSize;
    this.writeProperties = writeProperties;
    this.bootstrapSnapshotId = bootstrapSnapshotId;
    this.rebuildOnStaleRestore = rebuildOnStaleRestore;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    Preconditions.checkState(
        subtaskIndex < (1 << SUBTASK_BITS), "POC: index-dv supports < 4096 writer subtasks");

    TableLoader loader = tableLoader.clone();
    loader.open();
    this.table = loader.loadTable();
    this.spec = table.spec();
    Schema schema = table.schema();
    this.fileWriterFactory =
        new FlinkFileWriterFactory.Builder(table)
            .dataFileFormat(format)
            .dataSchema(schema)
            .dataFlinkType(flinkRowType)
            .writerProperties(writeProperties)
            .build();
    this.fileFactory =
        OutputFileFactory.builderFor(
                table, subtaskIndex, getRuntimeContext().getTaskInfo().getAttemptNumber())
            .format(format)
            .defaultSpec(spec)
            .build();
    this.wrapper = new RowDataWrapper(flinkRowType, schema.asStruct());
    this.partitionKey = new PartitionKey(spec, schema);
    this.serializer = new StructLikeSerializer();
    this.dataWriters = StructLikeMap.create(spec.partitionType());

    this.location = context.getKeyedStateStore().getState(LOCATION);
    this.fileDictionaryState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>(
                    "index-dv-files", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    this.files = Maps.newHashMap();
    this.fileIds = Maps.newHashMap();
    int maxCounter = -1;
    for (byte[] encoded : fileDictionaryState.get()) {
      FileEntry entry = decode(encoded);
      files.put(entry.id(), entry);
      if ((entry.id() & ((1 << SUBTASK_BITS) - 1)) == subtaskIndex) {
        maxCounter = Math.max(maxCounter, entry.id() >>> SUBTASK_BITS);
      }
    }

    this.nextFileCounter = maxCounter + 1;
    for (FileEntry entry : files.values()) {
      fileIds.put(entry.path(), entry.id());
    }

    this.lastSeenSnapshotState =
        context
            .getOperatorStateStore()
            .getUnionListState(new ListStateDescriptor<>("index-dv-last-seen", Types.LONG));
    for (Long seen : lastSeenSnapshotState.get()) {
      // Re-applying a rewrite is harmless, so after a rescale resume from the oldest subtask view.
      lastSeenSnapshotId = lastSeenSnapshotId == null ? seen : Math.min(lastSeenSnapshotId, seen);
    }

    this.unconfirmedState =
        context
            .getOperatorStateStore()
            .getUnionListState(new ListStateDescriptor<>("index-dv-unconfirmed", Types.STRING));
    this.unconfirmed = Sets.newHashSet(unconfirmedState.get());

    if (context.isRestored()) {
      reconcileRestoredIndex();
    } else {
      bootstrap(bootstrapSnapshotId);
      this.lastSeenSnapshotId = bootstrapSnapshotId;
    }
  }

  /**
   * Checks that the restored index has seen every data file in the table, which fails when the
   * state is older than the table: an abandoned run or another job committed after it. Upserts from
   * such an index would leave those rows live next to the new ones. The job fails, or with {@code
   * upsert-stale-restore=rebuild} the index is rebuilt from the head.
   *
   * <p>A data file is seen when the restored file dictionary knows it: bootstrapped, written by
   * this sink up to the restored checkpoint (committed or not), or registered by a rewrite. That
   * makes this sink's own commits after the restored head, such as the restored checkpoint
   * committed before its acknowledgement was lost, pass. Rewrites are reconciled at the next
   * barrier and skipped here; the rows they carry come from files added by snapshots that are
   * checked, or that expiry removed, which breaks the history and fails the check.
   *
   * <p>shortcut: snapshots that only remove data files are not checked.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void reconcileRestoredIndex() throws Exception {
    Snapshot head = table.snapshot(branch);
    if (head == null || (lastSeenSnapshotId != null && head.snapshotId() == lastSeenSnapshotId)) {
      return;
    }

    Set<String> added = Sets.newHashSet();
    Set<String> rewritten = Sets.newHashSet();
    String stale = null;
    int newer = 0;
    boolean reachedLastSeen = false;
    for (Snapshot snapshot = head; snapshot != null; newer++) {
      boolean replace = DataOperations.REPLACE.equals(snapshot.operation());
      if (!replace || rebuildOnStaleRestore) {
        SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
        if (replace) {
          changes.removedDataFiles().forEach(file -> rewritten.add(file.location()));
        } else {
          for (DataFile file : changes.addedDataFiles()) {
            added.add(file.location());
            if (stale == null && !fileIds.containsKey(file.location())) {
              stale =
                  String.format(
                      "Restored index-dv state is behind table %s branch %s: the index reflects"
                          + " snapshot %s, the head is %s, and snapshot %s (operation %s, %s=%s,"
                          + " %s=%s) added data file %s that the index has never seen. Upserts"
                          + " would leave those rows live as duplicates.",
                      table.name(),
                      branch,
                      lastSeenSnapshotId,
                      head.snapshotId(),
                      snapshot.snapshotId(),
                      snapshot.operation(),
                      SinkUtil.FLINK_JOB_ID,
                      snapshot.summary().get(SinkUtil.FLINK_JOB_ID),
                      SinkUtil.MAX_COMMITTED_CHECKPOINT_ID,
                      snapshot.summary().get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID),
                      file.location());
            }
          }
        }
      }

      if (stale != null && !rebuildOnStaleRestore) {
        break;
      }

      Long parentId = snapshot.parentId();
      if (parentId != null && parentId.equals(lastSeenSnapshotId)) {
        reachedLastSeen = true;
        break;
      }

      snapshot = parentId == null ? null : table.snapshot(parentId);
    }

    if (stale == null && lastSeenSnapshotId != null && !reachedLastSeen) {
      stale =
          String.format(
              "Cannot verify restored index-dv state for table %s branch %s: snapshot %s, which"
                  + " the index reflects, is not an ancestor of head %s or its history was"
                  + " expired.",
              table.name(), branch, lastSeenSnapshotId, head.snapshotId());
    }

    if (stale == null) {
      LOG.info(
          "Restored index-dv state on subtask {} reflects snapshot {}; the {} newer snapshot(s) up"
              + " to head {} add only data files the index knows",
          subtaskIndex,
          lastSeenSnapshotId,
          newer + 1,
          head.snapshotId());
      RESTORES_PAST_HEAD.incrementAndGet();
      return;
    }

    if (!rebuildOnStaleRestore) {
      throw new IllegalStateException(
          stale
              + " Start the job without state so the index bootstraps from the head, restore the"
              + " checkpoint or savepoint that matches the head, or set"
              + " upsert-stale-restore=rebuild.");
    }

    rebuild(head, stale, added, rewritten);
  }

  /**
   * Discards the restored index and indexes the live rows of the head instead.
   *
   * <p>Refused while the restored state holds results this sink wrote that the table does not hold:
   * the restored committer would commit them after the rebuild, with deletes resolved against the
   * stale index and rows the rebuilt index has not seen.
   */
  private void rebuild(Snapshot head, String stale, Set<String> added, Set<String> rewritten)
      throws Exception {
    Set<String> uncommitted = Sets.newHashSet(unconfirmed);
    uncommitted.removeAll(added);
    uncommitted.removeAll(rewritten);
    if (!uncommitted.isEmpty()) {
      try (CloseableIterable<FileScanTask> tasks =
          table.newScan().useSnapshot(head.snapshotId()).planFiles()) {
        for (FileScanTask task : tasks) {
          uncommitted.remove(task.file().location());
        }
      }
    }

    Preconditions.checkState(
        uncommitted.isEmpty(),
        "%s Cannot rebuild the index: the restored state holds %s data file(s) this sink wrote"
            + " that table %s does not hold, for example %s, and the restored committer would"
            + " commit them over the rebuilt index. Restore the checkpoint or savepoint that"
            + " matches the head, or start the job without state.",
        stale,
        uncommitted.size(),
        table.name(),
        uncommitted.isEmpty() ? null : uncommitted.iterator().next());

    Long restored = lastSeenSnapshotId;
    long start = System.currentTimeMillis();
    this.<SerializedEqualityValues>getKeyedStateBackend()
        .applyToAllKeys(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            LOCATION,
            (key, state) -> state.clear());
    files.clear();
    fileIds.clear();
    this.nextFileCounter = 0;
    long keys = bootstrap(head.snapshotId());
    this.lastSeenSnapshotId = head.snapshotId();
    long millis = System.currentTimeMillis() - start;
    REBUILDS.incrementAndGet();
    LAST_REBUILD_MILLIS.accumulateAndGet(millis, Math::max);
    LOG.warn(
        "Rebuilt the index-dv index on subtask {} from head {} of table {} branch {}: {} keys in {}"
            + " ms, discarding the restored index, which reflected snapshot {}. {}",
        subtaskIndex,
        head.snapshotId(),
        table.name(),
        branch,
        keys,
        millis,
        restored,
        stale);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    RowData row = element.getValue();
    switch (row.getRowKind()) {
      case INSERT, UPDATE_AFTER -> {
        retire();
        append(row);
      }
      case DELETE -> retire();
      case UPDATE_BEFORE -> {
        // upsert: the UPDATE_AFTER retires the previous row by key
      }
    }
  }

  private void retire() throws IOException {
    Long packed = location.value();
    if (packed != null) {
      FileEntry file = files.get((int) (packed >>> 32));
      output.collect(
          DELETES,
          new StreamRecord<>(
              new DVPosition(
                  file.path(), packed & MAX_POSITION, file.specId(), file.partition(), 0)));
      location.clear();
    }
  }

  private void append(RowData row) throws IOException {
    partitionKey.partition(wrapper.wrap(row));
    RollingDataWriter<RowData> writer = dataWriters.get(partitionKey);
    if (writer == null) {
      StructLike partition = spec.isUnpartitioned() ? null : partitionKey.copy();
      writer =
          new RollingDataWriter<>(
              fileWriterFactory, fileFactory, table.io(), targetFileSize, spec, partition);
      dataWriters.put(partitionKey.copy(), writer);
    }

    String path = writer.currentFilePath().toString();
    long pos = writer.currentFileRows();
    writer.write(row);
    Integer fileId = fileIds.get(path);
    if (fileId == null) {
      fileId = register(path, spec, partitionKey);
    }

    location.update(pack(fileId, pos));
  }

  private int register(String path, PartitionSpec fileSpec, StructLike partition) {
    int id = (nextFileCounter++ << SUBTASK_BITS) | subtaskIndex;
    Preconditions.checkState(id >= 0, "POC: index-dv file id space exhausted");
    files.put(
        id,
        new FileEntry(
            id,
            path,
            fileSpec.specId(),
            serializer.encodePartition(partition, fileSpec.partitionType())));
    fileIds.put(path, id);
    return id;
  }

  private static long pack(int fileId, long pos) {
    Preconditions.checkState(pos <= MAX_POSITION, "POC: row position %s too large", pos);
    return ((long) fileId << 32) | pos;
  }

  /**
   * Indexes the keys this subtask owns from the live rows of the bootstrap snapshot.
   *
   * <p>shortcut: every subtask reads the key columns of every data file and keeps its own key
   * groups; upgrade to a split read with a shuffle, incremental catch-up (#18717) or a shared index
   * extract (#16961).
   */
  private long bootstrap(Long snapshotId) throws IOException {
    if (snapshotId == null) {
      return 0;
    }

    long start = System.currentTimeMillis();
    long[] keys = {0};
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        for (DeleteFile delete : task.deletes()) {
          Preconditions.checkState(
              delete.content() != FileContent.POSITION_DELETES || ContentFileUtil.isDV(delete),
              "POC: index-dv upsert mode does not support V2 position delete files: %s",
              delete.location());
        }

        int fileId = register(file.location(), table.specs().get(file.specId()), file.partition());
        forEachOwnedKey(
            file,
            task,
            (key, pos) -> {
              location.update(pack(fileId, pos));
              keys[0]++;
            });
      }
    }

    long millis = System.currentTimeMillis() - start;
    LAST_BOOTSTRAP_MILLIS.accumulateAndGet(millis, Math::max);
    BOOTSTRAPPED_KEYS.addAndGet(keys[0]);
    LOG.info("Bootstrapped {} keys on subtask {} in {} ms", keys[0], subtaskIndex, millis);
    return keys[0];
  }

  /**
   * Visits the keys this subtask owns in a data file with their positions, as the current key of
   * the keyed state. With a scan task, rows deleted in that task are skipped.
   */
  private void forEachOwnedKey(DataFile file, FileScanTask task, KeyPositionConsumer consumer)
      throws IOException {
    Schema keySchema = TypeUtil.select(table.schema(), equalityFieldIds);
    List<NestedField> columns = Lists.newArrayList(keySchema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    Schema requested = new Schema(columns);
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keySchema.asStruct());
    int maxParallelism = getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();
    KeyGroupRange ownedKeyGroups =
        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism,
            getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
            subtaskIndex);

    GenericDeleteFilter filter =
        task != null ? new GenericDeleteFilter(table.io(), task, table.schema(), requested) : null;
    Schema readSchema = filter != null ? filter.requiredSchema() : requested;
    StructProjection keyProjection = StructProjection.create(readSchema, keySchema);
    Accessor<StructLike> posAccessor =
        readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    CloseableIterable<Record> records =
        FormatModelRegistry.<Record, Schema>readBuilder(
                file.format(), Record.class, table.io().newInputFile(file.location()))
            .project(readSchema)
            .build();
    try (CloseableIterable<Record> rows = filter != null ? filter.filter(records) : records) {
      for (Record record : rows) {
        SerializedEqualityValues key =
            IndexDvKeySelector.prefixed(
                serializer.serializeKey(
                    keyWrapper.wrap(keyProjection.wrap(record)), keySchema.asStruct()));
        if (ownedKeyGroups.contains(
            KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism))) {
          getKeyedStateBackend().setCurrentKey(key);
          consumer.accept(key, (long) posAccessor.get(record));
        }
      }
    }
  }

  @FunctionalInterface
  private interface KeyPositionConsumer {
    void accept(SerializedEqualityValues key, long pos) throws IOException;
  }

  /**
   * Moves keys whose row a rewrite (compaction) carried into a new data file to that file, so later
   * updates delete the live copy. Keys updated since the rewrite already point elsewhere and stay.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void applyRewrites() throws IOException {
    table.refresh();
    Snapshot head = table.snapshot(branch);
    if (head == null || lastSeenSnapshotId == null || head.snapshotId() == lastSeenSnapshotId) {
      this.lastSeenSnapshotId = head != null ? head.snapshotId() : null;
      return;
    }

    List<Snapshot> newer = Lists.newArrayList();
    for (Snapshot snapshot = head;
        snapshot != null && snapshot.snapshotId() != lastSeenSnapshotId;
        snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId())) {
      newer.add(0, snapshot);
    }

    for (Snapshot snapshot : newer) {
      if (!DataOperations.REPLACE.equals(snapshot.operation())) {
        if (rebuildOnStaleRestore && !unconfirmed.isEmpty()) {
          SnapshotChanges.builderFor(table)
              .snapshot(snapshot)
              .build()
              .addedDataFiles()
              .forEach(file -> unconfirmed.remove(file.location()));
        }

        continue;
      }

      SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      Set<Integer> removed = Sets.newHashSet();
      for (DataFile file : changes.removedDataFiles()) {
        Integer id = fileIds.get(file.location());
        if (id != null) {
          removed.add(id);
        }
      }

      if (removed.isEmpty()) {
        continue;
      }

      long moved = 0;
      for (DataFile file : changes.addedDataFiles()) {
        int fileId =
            fileIds.containsKey(file.location())
                ? fileIds.get(file.location())
                : register(file.location(), table.specs().get(file.specId()), file.partition());
        long[] count = {0};
        forEachOwnedKey(
            file,
            null,
            (key, pos) -> {
              Long current = location.value();
              if (current != null && removed.contains((int) (current >>> 32))) {
                location.update(pack(fileId, pos));
                count[0]++;
              }
            });
        moved += count[0];
      }

      REWRITES_APPLIED.incrementAndGet();
      LOG.info(
          "Applied rewrite snapshot {} on subtask {}: moved {} keys",
          snapshot.snapshotId(),
          subtaskIndex,
          moved);
    }

    this.lastSeenSnapshotId = head.snapshotId();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    if (!inputEnded) {
      applyRewrites();
    }

    flush(checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // shortcut: the whole dictionary on every checkpoint; upgrade to GC of unreferenced files
    List<byte[]> encoded = Lists.newArrayListWithCapacity(files.size());
    for (FileEntry entry : files.values()) {
      encoded.add(encode(entry));
    }

    fileDictionaryState.update(encoded);
    lastSeenSnapshotState.update(
        lastSeenSnapshotId != null ? ImmutableList.of(lastSeenSnapshotId) : ImmutableList.of());
    unconfirmedState.update(Lists.newArrayList(unconfirmed));
  }

  @Override
  public void endInput() throws IOException {
    flush(IcebergStreamWriter.END_INPUT_CHECKPOINT_ID);
    this.inputEnded = true;
  }

  private void flush(long checkpointId) throws IOException {
    if (inputEnded) {
      return;
    }

    List<DataFile> dataFiles = Lists.newArrayList();
    for (RollingDataWriter<RowData> writer : dataWriters.values()) {
      writer.close();
      dataFiles.addAll(writer.result().dataFiles());
    }

    dataWriters.clear();
    if (rebuildOnStaleRestore) {
      dataFiles.forEach(file -> unconfirmed.add(file.location()));
    }

    output.collect(
        new StreamRecord<>(
            new FlinkWriteResult(
                checkpointId, WriteResult.builder().addDataFiles(dataFiles).build())));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (dataWriters != null) {
      for (RollingDataWriter<RowData> writer : dataWriters.values()) {
        writer.close();
      }
    }
  }

  private static byte[] encode(FileEntry entry) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(entry.id());
      out.writeUTF(entry.path());
      out.writeInt(entry.specId());
      out.writeInt(entry.partition().length);
      out.write(entry.partition());
    }

    return bytes.toByteArray();
  }

  private static FileEntry decode(byte[] encoded) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
      int id = in.readInt();
      String path = in.readUTF();
      int specId = in.readInt();
      byte[] partition = new byte[in.readInt()];
      in.readFully(partition);
      return new FileEntry(id, path, specId, partition);
    }
  }
}
