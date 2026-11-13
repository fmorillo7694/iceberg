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
package org.apache.iceberg.flink.sink.dynamic;

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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
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
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.flink.sink.FlinkFileWriterFactory;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC: the upsert writer of the dynamic sink's index-dv mode, keyed by (table, branch, equality
 * key). It writes the data files itself and keeps each key's live row position in keyed state. A
 * table is set up on its first record on a subtask: the subtask indexes the keys it owns from the
 * branch head, and after a restore first checks that the restored index is not behind the table.
 */
class DynamicIndexDvDataWriter extends AbstractStreamOperator<byte[]>
    implements OneInputStreamOperator<DynamicRecordInternal, byte[]>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicIndexDvDataWriter.class);

  static final OutputTag<DynamicDvPosition> DELETES =
      new OutputTag<>("dynamic-index-dv-deletes", TypeInformation.of(DynamicDvPosition.class));
  static final TypeInformation<byte[]> RESULT_TYPE =
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

  private static final int SUBTASK_BITS = 12;
  private static final long MAX_POSITION = 0xFFFFFFFFL;

  @VisibleForTesting static final AtomicLong BOOTSTRAPPED_KEYS = new AtomicLong();
  @VisibleForTesting static final AtomicLong TABLE_BOOTSTRAPS = new AtomicLong();
  @VisibleForTesting static final AtomicLong REWRITES_APPLIED = new AtomicLong();

  private final CatalogLoader catalogLoader;
  private final Map<String, String> writeProperties;
  private final Configuration flinkConfig;

  private transient Catalog catalog;
  private transient StructLikeSerializer serializer;
  private transient ValueState<Long> location;
  private transient ListState<byte[]> fileDictionaryState;
  private transient ListState<byte[]> tableStates;
  private transient Map<Integer, FileEntry> files;
  private transient Map<String, Integer> fileIds;
  private transient Map<TableKey, RestoredTable> restoredTables;
  private transient Map<TableKey, ActiveTable> activeTables;
  private transient KeyGroupRange ownedKeyGroups;
  private transient int maxParallelism;
  private transient int subtaskIndex;
  private transient int nextFileCounter;
  private transient boolean inputEnded;

  DynamicIndexDvDataWriter(
      CatalogLoader catalogLoader, Map<String, String> writeProperties, Configuration flinkConfig) {
    this.catalogLoader = catalogLoader;
    this.writeProperties = writeProperties;
    this.flinkConfig = flinkConfig;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    Preconditions.checkState(
        subtaskIndex < (1 << SUBTASK_BITS), "POC: index-dv supports < 4096 writer subtasks");
    this.maxParallelism = getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();
    this.ownedKeyGroups =
        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism,
            getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
            subtaskIndex);
    this.catalog = catalogLoader.loadCatalog();
    this.serializer = new StructLikeSerializer();
    this.activeTables = Maps.newHashMap();

    this.location =
        context
            .getKeyedStateStore()
            .getState(new ValueStateDescriptor<>("dynamic-index-dv-location", Types.LONG));
    this.fileDictionaryState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>(
                    "dynamic-index-dv-files",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    this.files = Maps.newHashMap();
    this.fileIds = Maps.newHashMap();
    int maxCounter = -1;
    for (byte[] encoded : fileDictionaryState.get()) {
      FileEntry entry = FileEntry.decode(encoded);
      files.put(entry.id, entry);
      fileIds.put(entry.path, entry.id);
      if ((entry.id & ((1 << SUBTASK_BITS) - 1)) == subtaskIndex) {
        maxCounter = Math.max(maxCounter, entry.id >>> SUBTASK_BITS);
      }
    }

    this.nextFileCounter = maxCounter + 1;

    this.tableStates =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>(
                    "dynamic-index-dv-tables",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    this.restoredTables = Maps.newHashMap();
    for (byte[] encoded : tableStates.get()) {
      RestoredTable table = RestoredTable.decode(encoded);
      restoredTables.merge(table.key, table, RestoredTable::merge);
    }
  }

  @Override
  public void processElement(StreamRecord<DynamicRecordInternal> element) throws Exception {
    DynamicRecordInternal record = element.getValue();
    // A bootstrap moves the keyed state to the keys it indexes; this record keeps its own key.
    Object recordKey = getCurrentKey();
    ActiveTable table = activate(record);
    setCurrentKey(recordKey);
    Preconditions.checkState(
        table.equalityFieldIds.equals(record.equalityFields()),
        "POC: equality fields of table %s changed from %s to %s; index-dv cannot follow a key"
            + " change",
        table.key.tableName(),
        table.equalityFieldIds,
        record.equalityFields());
    RowData row = record.rowData();
    switch (row.getRowKind()) {
      case INSERT, UPDATE_AFTER -> {
        retire();
        append(table, record);
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
              new DynamicDvPosition(
                  file.tableName,
                  file.branch,
                  file.path,
                  packed & MAX_POSITION,
                  file.specId,
                  file.partition)));
      location.clear();
    }
  }

  private void append(ActiveTable table, DynamicRecordInternal record) throws IOException {
    Schema schema = record.schema();
    PartitionSpec spec = record.spec();
    TargetWriters target =
        table.targets.computeIfAbsent(
            schema.schemaId() + "/" + spec.specId(), unused -> table.newTarget(schema, spec));
    target.partitionKey.partition(target.wrapper.wrap(record.rowData()));
    RollingDataWriter<RowData> writer = target.writers.get(target.partitionKey);
    if (writer == null) {
      StructLike partition = spec.isUnpartitioned() ? null : target.partitionKey.copy();
      writer =
          new RollingDataWriter<>(
              target.fileWriterFactory,
              table.fileFactory,
              table.table.io(),
              table.targetFileSize,
              spec,
              partition);
      target.writers.put(target.partitionKey.copy(), writer);
    }

    String path = writer.currentFilePath().toString();
    long pos = writer.currentFileRows();
    writer.write(record.rowData());
    Integer fileId = fileIds.get(path);
    if (fileId == null) {
      fileId = register(table.key, path, spec, target.partitionKey);
    }

    location.update(pack(fileId, pos));
    table.dirty = true;
  }

  /**
   * Sets a table up on its first record on this subtask: loads it, checks a restored index against
   * the table, and indexes the owned keys of key groups no subtask has indexed yet.
   */
  private ActiveTable activate(DynamicRecordInternal record) throws IOException {
    TableKey key = new TableKey(record.tableName(), record.branch());
    ActiveTable active = activeTables.get(key);
    if (active != null) {
      return active;
    }

    Table table = catalog.loadTable(TableIdentifier.parse(key.tableName()));
    Preconditions.checkState(
        TableUtil.formatVersion(table) >= 3,
        "Upsert mode 'index-dv' requires format version 3 or later, table %s is version %s",
        table.name(),
        TableUtil.formatVersion(table));
    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
    Preconditions.checkState(
        record.equalityFields().equals(identifierFieldIds),
        "POC: index-dv in the dynamic sink requires the equality fields %s of table %s to be its"
            + " identifier fields %s",
        record.equalityFields(),
        table.name(),
        identifierFieldIds);

    RestoredTable restored = restoredTables.get(key);
    active = new ActiveTable(key, table, record.equalityFields());
    boolean[] covered = new boolean[maxParallelism];
    if (restored != null) {
      Preconditions.checkState(
          restored.equalityFieldIds.equals(record.equalityFields()),
          "POC: equality fields of table %s changed from %s to %s since the restored index was"
              + " built; index-dv cannot follow a key change",
          table.name(),
          restored.equalityFieldIds,
          record.equalityFields());
      active.lastSeenSnapshotId = restored.lastSeenSnapshotId;
      active.coveredRanges.addAll(restored.coveredRanges);
      for (int[] range : restored.coveredRanges) {
        for (int group = range[0]; group <= range[1]; group++) {
          covered[group] = true;
        }
      }

      checkRestoredIndexIsCurrent(active);
    }

    boolean missing = false;
    for (int group = ownedKeyGroups.getStartKeyGroup();
        group <= ownedKeyGroups.getEndKeyGroup();
        group++) {
      missing |= !covered[group];
    }

    if (missing) {
      Snapshot head = table.snapshot(key.branch());
      bootstrap(active, head, covered);
      active.coveredRanges.add(
          new int[] {ownedKeyGroups.getStartKeyGroup(), ownedKeyGroups.getEndKeyGroup()});
      if (restored == null) {
        active.lastSeenSnapshotId = head != null ? head.snapshotId() : null;
      }
    }

    activeTables.put(key, active);
    return active;
  }

  /** Fails when the table holds data files the restored index of this table has never seen. */
  private void checkRestoredIndexIsCurrent(ActiveTable active) {
    Long lastSeen = active.lastSeenSnapshotId;
    Snapshot head = active.table.snapshot(active.key.branch());
    if (head == null || (lastSeen != null && head.snapshotId() == lastSeen)) {
      return;
    }

    for (Snapshot snapshot = head; snapshot != null; ) {
      if (!DataOperations.REPLACE.equals(snapshot.operation())) {
        for (DataFile file :
            SnapshotChanges.builderFor(active.table).snapshot(snapshot).build().addedDataFiles()) {
          Preconditions.checkState(
              fileIds.containsKey(file.location()),
              "Restored index-dv state of table %s branch %s is behind the table: the index"
                  + " reflects snapshot %s, the head is %s, and snapshot %s (operation %s) added"
                  + " data file %s that the index has never seen. Upserts would leave those rows"
                  + " live as duplicates. Start the job without state, or restore the checkpoint"
                  + " or savepoint that matches the head.",
              active.table.name(),
              active.key.branch(),
              lastSeen,
              head.snapshotId(),
              snapshot.snapshotId(),
              snapshot.operation(),
              file.location());
        }
      }

      Long parentId = snapshot.parentId();
      if (parentId != null && parentId.equals(lastSeen)) {
        return;
      }

      snapshot = parentId == null ? null : active.table.snapshot(parentId);
    }

    Preconditions.checkState(
        lastSeen == null,
        "Cannot verify restored index-dv state of table %s branch %s: snapshot %s, which the index"
            + " reflects, is not an ancestor of head %s or its history was expired. Start the job"
            + " without state.",
        active.table.name(),
        active.key.branch(),
        lastSeen,
        head.snapshotId());
  }

  /**
   * Indexes the live rows of a snapshot whose keys fall in key groups this subtask owns and no
   * subtask has indexed yet.
   *
   * <p>shortcut: every subtask reads the key columns of every data file of the table; upgrade to a
   * split read or a shared index extract (#16961).
   */
  private void bootstrap(ActiveTable active, Snapshot snapshot, boolean[] covered)
      throws IOException {
    if (snapshot == null) {
      return;
    }

    long start = System.currentTimeMillis();
    long[] keys = {0};
    try (CloseableIterable<FileScanTask> tasks =
        active.table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        for (DeleteFile delete : task.deletes()) {
          Preconditions.checkState(
              delete.content() != FileContent.POSITION_DELETES || ContentFileUtil.isDV(delete),
              "POC: index-dv upsert mode does not support V2 position delete files: %s",
              delete.location());
        }

        Integer known = fileIds.get(file.location());
        int fileId =
            known != null
                ? known
                : register(
                    active.key,
                    file.location(),
                    active.table.specs().get(file.specId()),
                    file.partition());
        forEachOwnedKey(
            active,
            file,
            task,
            group -> !covered[group],
            pos -> {
              location.update(pack(fileId, pos));
              keys[0]++;
            });
      }
    }

    BOOTSTRAPPED_KEYS.addAndGet(keys[0]);
    TABLE_BOOTSTRAPS.incrementAndGet();
    LOG.info(
        "Bootstrapped {} keys of table {} branch {} on subtask {} from snapshot {} in {} ms",
        keys[0],
        active.table.name(),
        active.key.branch(),
        subtaskIndex,
        snapshot.snapshotId(),
        System.currentTimeMillis() - start);
  }

  /**
   * Visits the keys of a data file that this subtask owns and whose key group passes the filter, as
   * the current key of the keyed state. With a scan task, rows deleted in that task are skipped.
   */
  private void forEachOwnedKey(
      ActiveTable active,
      DataFile file,
      FileScanTask task,
      KeyGroupFilter groups,
      PositionConsumer consumer)
      throws IOException {
    Schema tableSchema = active.table.schema();
    Schema keySchema = DynamicIndexDvKeys.keySchema(tableSchema, active.equalityFieldIds);
    List<NestedField> columns = Lists.newArrayList(keySchema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    Schema requested = new Schema(columns);
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keySchema.asStruct());
    GenericDeleteFilter filter =
        task != null
            ? new GenericDeleteFilter(active.table.io(), task, tableSchema, requested)
            : null;
    Schema readSchema = filter != null ? filter.requiredSchema() : requested;
    StructProjection keyProjection = StructProjection.create(readSchema, keySchema);
    Accessor<StructLike> posAccessor =
        readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    CloseableIterable<Record> records =
        FormatModelRegistry.<Record, Schema>readBuilder(
                file.format(), Record.class, active.table.io().newInputFile(file.location()))
            .project(readSchema)
            .build();
    try (CloseableIterable<Record> rows = filter != null ? filter.filter(records) : records) {
      for (Record record : rows) {
        SerializedEqualityValues key =
            DynamicIndexDvKeys.compose(
                active.key,
                serializer.serializeKey(
                    keyWrapper.wrap(keyProjection.wrap(record)), keySchema.asStruct()));
        int group = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
        if (ownedKeyGroups.contains(group) && groups.accept(group)) {
          getKeyedStateBackend().setCurrentKey(key);
          consumer.accept((long) posAccessor.get(record));
        }
      }
    }
  }

  @FunctionalInterface
  private interface KeyGroupFilter {
    boolean accept(int keyGroup);
  }

  @FunctionalInterface
  private interface PositionConsumer {
    void accept(long pos) throws IOException;
  }

  /**
   * Moves keys whose row a rewrite carried into a new data file to that file, for the tables
   * written since the last barrier. The committer rebases DVs that still point at removed files.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void applyRewrites(ActiveTable active) throws IOException {
    Table table = active.table;
    table.refresh();
    Snapshot head = table.snapshot(active.key.branch());
    Long lastSeen = active.lastSeenSnapshotId;
    if (head == null || lastSeen == null || head.snapshotId() == lastSeen) {
      active.lastSeenSnapshotId = head != null ? head.snapshotId() : null;
      return;
    }

    List<Snapshot> newer = Lists.newArrayList();
    for (Snapshot snapshot = head;
        snapshot != null && snapshot.snapshotId() != lastSeen;
        snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId())) {
      newer.add(0, snapshot);
    }

    for (Snapshot snapshot : newer) {
      if (!DataOperations.REPLACE.equals(snapshot.operation())) {
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

      for (DataFile file : changes.addedDataFiles()) {
        Integer known = fileIds.get(file.location());
        int fileId =
            known != null
                ? known
                : register(
                    active.key,
                    file.location(),
                    table.specs().get(file.specId()),
                    file.partition());
        forEachOwnedKey(
            active,
            file,
            null,
            group -> true,
            pos -> {
              Long current = location.value();
              if (current != null && removed.contains((int) (current >>> 32))) {
                location.update(pack(fileId, pos));
              }
            });
      }

      REWRITES_APPLIED.incrementAndGet();
    }

    active.lastSeenSnapshotId = head.snapshotId();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    if (!inputEnded) {
      for (ActiveTable active : activeTables.values()) {
        if (active.dirty) {
          applyRewrites(active);
        }
      }
    }

    flush();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // shortcut: the whole dictionary on every checkpoint; upgrade to GC of unreferenced files
    List<byte[]> encoded = Lists.newArrayListWithCapacity(files.size());
    for (FileEntry entry : files.values()) {
      encoded.add(entry.encode());
    }

    fileDictionaryState.update(encoded);
    Map<TableKey, RestoredTable> known = Maps.newHashMap(restoredTables);
    for (ActiveTable active : activeTables.values()) {
      known.put(
          active.key,
          new RestoredTable(
              active.key,
              active.lastSeenSnapshotId,
              active.equalityFieldIds,
              active.coveredRanges));
    }

    List<byte[]> tables = Lists.newArrayListWithCapacity(known.size());
    for (RestoredTable table : known.values()) {
      tables.add(table.encode());
    }

    tableStates.update(tables);
  }

  @Override
  public void endInput() throws IOException {
    flush();
    this.inputEnded = true;
  }

  private void flush() throws IOException {
    if (inputEnded) {
      return;
    }

    for (ActiveTable active : activeTables.values()) {
      Map<Integer, List<DataFile>> filesBySpec = Maps.newHashMap();
      for (TargetWriters target : active.targets.values()) {
        for (DataFile file : target.complete()) {
          filesBySpec.computeIfAbsent(file.specId(), id -> Lists.newArrayList()).add(file);
        }
      }

      for (Map.Entry<Integer, List<DataFile>> entry : filesBySpec.entrySet()) {
        output.collect(
            new StreamRecord<>(
                encode(
                    new DynamicWriteResult(
                        active.key,
                        entry.getKey(),
                        WriteResult.builder().addDataFiles(entry.getValue()).build()))));
      }

      active.dirty = false;
    }
  }

  static byte[] encode(DynamicWriteResult result) throws IOException {
    return SimpleVersionedSerialization.writeVersionAndSerialize(
        new DynamicWriteResultSerializer(), result);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (activeTables != null) {
      for (ActiveTable active : activeTables.values()) {
        for (TargetWriters target : active.targets.values()) {
          target.close();
        }
      }
    }
  }

  private int register(TableKey table, String path, PartitionSpec spec, StructLike partition) {
    int id = (nextFileCounter++ << SUBTASK_BITS) | subtaskIndex;
    Preconditions.checkState(id >= 0, "POC: index-dv file id space exhausted");
    files.put(
        id,
        new FileEntry(
            id,
            table.tableName(),
            table.branch(),
            path,
            spec.specId(),
            serializer.encodePartition(partition, spec.partitionType())));
    fileIds.put(path, id);
    return id;
  }

  private static long pack(int fileId, long pos) {
    Preconditions.checkState(pos <= MAX_POSITION, "POC: row position %s too large", pos);
    return ((long) fileId << 32) | pos;
  }

  /** A table this subtask has seen in this run, with its writers. */
  private final class ActiveTable {
    private final TableKey key;
    private final Table table;
    private final Set<Integer> equalityFieldIds;
    private final List<int[]> coveredRanges = Lists.newArrayList();
    private final Map<String, TargetWriters> targets = Maps.newHashMap();
    private final Map<Integer, FileWriterFactory<RowData>> writerFactories = Maps.newHashMap();
    private final OutputFileFactory fileFactory;
    private final FileFormat format;
    private final long targetFileSize;
    private final Map<String, String> tableWriteProperties;
    private Long lastSeenSnapshotId;
    private boolean dirty;

    private ActiveTable(TableKey key, Table table, Set<Integer> equalityFieldIds) {
      this.key = key;
      this.table = table;
      this.equalityFieldIds = equalityFieldIds;
      FlinkWriteConf conf = new FlinkWriteConf(table, writeProperties, flinkConfig);
      this.format = conf.dataFileFormat();
      this.targetFileSize = conf.targetDataFileSize();
      this.tableWriteProperties = SinkUtil.writeProperties(format, conf, table);
      this.fileFactory =
          OutputFileFactory.builderFor(
                  table, subtaskIndex, getRuntimeContext().getTaskInfo().getAttemptNumber())
              .format(format)
              .build();
    }

    private TargetWriters newTarget(Schema schema, PartitionSpec spec) {
      FileWriterFactory<RowData> factory =
          writerFactories.computeIfAbsent(
              schema.schemaId(),
              id ->
                  new FlinkFileWriterFactory.Builder(table)
                      .dataFileFormat(format)
                      .dataSchema(schema)
                      .dataFlinkType(FlinkSchemaUtil.convert(schema))
                      .writerProperties(tableWriteProperties)
                      .build());
      return new TargetWriters(schema, spec, factory);
    }
  }

  /** The open data writers of one (schema, spec) of a table, one per partition. */
  private static final class TargetWriters {
    private final RowDataWrapper wrapper;
    private final PartitionKey partitionKey;
    private final FileWriterFactory<RowData> fileWriterFactory;
    private final Map<StructLike, RollingDataWriter<RowData>> writers;

    private TargetWriters(
        Schema schema, PartitionSpec spec, FileWriterFactory<RowData> fileWriterFactory) {
      this.wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
      this.partitionKey = new PartitionKey(spec, schema);
      this.fileWriterFactory = fileWriterFactory;
      this.writers = StructLikeMap.create(spec.partitionType());
    }

    private void close() throws IOException {
      for (RollingDataWriter<RowData> writer : writers.values()) {
        writer.close();
      }
    }

    /** Closes the open data files and returns them. */
    private List<DataFile> complete() throws IOException {
      List<DataFile> completed = Lists.newArrayList();
      for (RollingDataWriter<RowData> writer : writers.values()) {
        writer.close();
        completed.addAll(writer.result().dataFiles());
      }

      writers.clear();
      return completed;
    }
  }

  /** A data file whose rows the index can point at. */
  private static final class FileEntry {
    private final int id;
    private final String tableName;
    private final String branch;
    private final String path;
    private final int specId;
    private final byte[] partition;

    private FileEntry(
        int id, String tableName, String branch, String path, int specId, byte[] partition) {
      this.id = id;
      this.tableName = tableName;
      this.branch = branch;
      this.path = path;
      this.specId = specId;
      this.partition = partition;
    }

    private byte[] encode() throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeInt(id);
        out.writeUTF(tableName);
        out.writeUTF(branch);
        out.writeUTF(path);
        out.writeInt(specId);
        out.writeInt(partition.length);
        out.write(partition);
      }

      return bytes.toByteArray();
    }

    private static FileEntry decode(byte[] encoded) throws IOException {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
        int id = in.readInt();
        String tableName = in.readUTF();
        String branch = in.readUTF();
        String path = in.readUTF();
        int specId = in.readInt();
        byte[] partition = new byte[in.readInt()];
        in.readFully(partition);
        return new FileEntry(id, tableName, branch, path, specId, partition);
      }
    }
  }

  /** The per-table index metadata kept in union state: merged across subtasks on restore. */
  private static final class RestoredTable {
    private final TableKey key;
    private final Long lastSeenSnapshotId;
    private final Set<Integer> equalityFieldIds;
    private final List<int[]> coveredRanges;

    private RestoredTable(
        TableKey key,
        Long lastSeenSnapshotId,
        Set<Integer> equalityFieldIds,
        List<int[]> coveredRanges) {
      this.key = key;
      this.lastSeenSnapshotId = lastSeenSnapshotId;
      this.equalityFieldIds = equalityFieldIds;
      this.coveredRanges = coveredRanges;
    }

    /** Re-applying a rewrite is harmless, so after a rescale resume from the oldest view. */
    private static RestoredTable merge(RestoredTable one, RestoredTable other) {
      Preconditions.checkState(
          one.equalityFieldIds.equals(other.equalityFieldIds),
          "POC: subtasks disagree on the equality fields of table %s: %s and %s",
          one.key.tableName(),
          one.equalityFieldIds,
          other.equalityFieldIds);
      Long lastSeen;
      if (one.lastSeenSnapshotId == null) {
        lastSeen = other.lastSeenSnapshotId;
      } else if (other.lastSeenSnapshotId == null) {
        lastSeen = one.lastSeenSnapshotId;
      } else {
        lastSeen = Math.min(one.lastSeenSnapshotId, other.lastSeenSnapshotId);
      }

      List<int[]> ranges = Lists.newArrayList(one.coveredRanges);
      ranges.addAll(other.coveredRanges);
      return new RestoredTable(one.key, lastSeen, one.equalityFieldIds, ranges);
    }

    private byte[] encode() throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeUTF(key.tableName());
        out.writeUTF(key.branch());
        out.writeBoolean(lastSeenSnapshotId != null);
        out.writeLong(lastSeenSnapshotId != null ? lastSeenSnapshotId : 0L);
        out.writeInt(equalityFieldIds.size());
        for (int id : equalityFieldIds) {
          out.writeInt(id);
        }

        out.writeInt(coveredRanges.size());
        for (int[] range : coveredRanges) {
          out.writeInt(range[0]);
          out.writeInt(range[1]);
        }
      }

      return bytes.toByteArray();
    }

    private static RestoredTable decode(byte[] encoded) throws IOException {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
        TableKey key = new TableKey(in.readUTF(), in.readUTF());
        boolean hasLastSeen = in.readBoolean();
        long lastSeen = in.readLong();
        Set<Integer> ids = Sets.newHashSet();
        for (int i = in.readInt(); i > 0; i--) {
          ids.add(in.readInt());
        }

        List<int[]> ranges = Lists.newArrayList();
        for (int i = in.readInt(); i > 0; i--) {
          ranges.add(new int[] {in.readInt(), in.readInt()});
        }

        return new RestoredTable(key, hasLastSeen ? lastSeen : null, ids, ranges);
      }
    }
  }
}
