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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC writer for upserts that never writes equality deletes. Each subtask keeps the position of the
 * live row of every key it owns and retires a previous row with a deletion vector.
 *
 * <p>Requires the input to be keyed by the equality fields, a V3 table and an unchanged parallelism
 * across restores.
 */
class IndexDvSinkWriter
    implements CommittingSinkWriter<RowData, WriteResult>,
        StatefulSinkWriter<RowData, IndexDvWriterState> {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDvSinkWriter.class);

  private final Table table;
  private final Schema schema;
  private final PartitionSpec spec;
  private final Map<Integer, PartitionSpec> specs;
  private final long targetFileSize;
  private final int subtaskIndex;
  private final int parallelism;
  private final FileWriterFactory<RowData> fileWriterFactory;
  private final OutputFileFactory fileFactory;
  private final RowDataWrapper wrapper;
  private final StructProjection keyProjection;
  private final Types.StructType keyType;
  private final StructLikeSerializer keySerializer = new StructLikeSerializer();
  private final PartitionKey partitionKey;
  private final DeleteLoader deleteLoader;
  private final KeyPositionIndex index;

  private final Map<StructLike, RollingDataWriter<RowData>> dataWriters;
  private final Map<String, KeyPositionIndex.FileRef> fileRefs = Maps.newHashMap();
  private BaseDVFileWriter dvWriter;

  @SuppressWarnings("checkstyle:ParameterNumber")
  IndexDvSinkWriter(
      Table table,
      String branch,
      RowType flinkRowType,
      Set<Integer> equalityFieldIds,
      FileFormat format,
      long targetFileSize,
      Map<String, String> writeProperties,
      int subtaskIndex,
      int attemptId,
      int parallelism,
      int maxParallelism,
      Collection<IndexDvWriterState> restoredStates)
      throws IOException {
    this.table = table;
    this.schema = table.schema();
    this.spec = table.spec();
    this.specs = table.specs();
    this.targetFileSize = targetFileSize;
    this.subtaskIndex = subtaskIndex;
    this.parallelism = parallelism;
    this.fileWriterFactory =
        new FlinkFileWriterFactory.Builder(table)
            .dataFileFormat(format)
            .dataSchema(schema)
            .dataFlinkType(flinkRowType)
            .writerProperties(writeProperties)
            .build();
    this.fileFactory =
        OutputFileFactory.builderFor(table, subtaskIndex, attemptId)
            .format(format)
            .defaultSpec(spec)
            .build();
    this.wrapper = new RowDataWrapper(flinkRowType, schema.asStruct());
    Schema keySchema = TypeUtil.select(schema, equalityFieldIds);
    this.keyProjection = StructProjection.create(schema, keySchema);
    this.keyType = keySchema.asStruct();
    this.partitionKey = new PartitionKey(spec, schema);
    this.dataWriters = StructLikeMap.create(spec.partitionType());
    this.deleteLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));

    if (restoredStates.isEmpty()) {
      StructLikeWrapper hashWrapper = StructLikeWrapper.forType(keyType);
      this.index =
          KeyPositionIndex.bootstrap(
              table,
              branch,
              keySchema,
              key ->
                  KeyGroupRangeAssignment.assignKeyToParallelOperator(
                          hashWrapper.set(key).hashCode(), maxParallelism, parallelism)
                      == subtaskIndex,
              keySerializer);
      LOG.info("Bootstrapped {} keys for subtask {} from table", index.size(), subtaskIndex);
    } else {
      // shortcut: one blob per subtask, so rescaling would need a re-bootstrap or keyed state
      Preconditions.checkState(
          restoredStates.size() == 1,
          "POC: index-dv writer subtask %s restored %s states, rescaling is not supported",
          subtaskIndex,
          restoredStates.size());
      IndexDvWriterState state = restoredStates.iterator().next();
      Preconditions.checkState(
          state.subtaskIndex() == subtaskIndex && state.parallelism() == parallelism,
          "POC: index-dv writer state of subtask %s/%s restored on subtask %s/%s, "
              + "rescaling is not supported",
          state.subtaskIndex(),
          state.parallelism(),
          subtaskIndex,
          parallelism);
      this.index = KeyPositionIndex.deserialize(state.index(), specs);
      LOG.info("Restored {} keys for subtask {} from state", index.size(), subtaskIndex);
    }
  }

  @Override
  public void write(RowData row, Context context) throws IOException {
    StructLike wrapped = wrapper.wrap(row);
    SerializedEqualityValues key = keySerializer.serializeKey(keyProjection.wrap(wrapped), keyType);
    switch (row.getRowKind()) {
      case INSERT, UPDATE_AFTER -> {
        retire(key);
        append(wrapped, row, key);
      }
      case DELETE -> retire(key);
      case UPDATE_BEFORE -> {
        // upsert: the UPDATE_AFTER retires the previous row by key
      }
    }
  }

  private void retire(SerializedEqualityValues key) {
    KeyPositionIndex.Position previous = index.remove(key);
    if (previous != null) {
      KeyPositionIndex.FileRef file = previous.file();
      dvWriter().delete(file.path(), previous.pos(), specs.get(file.specId()), file.partition());
    }
  }

  private void append(StructLike wrapped, RowData row, SerializedEqualityValues key) {
    partitionKey.partition(wrapped);
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
    KeyPositionIndex.FileRef file =
        fileRefs.computeIfAbsent(
            path, p -> new KeyPositionIndex.FileRef(p, spec.specId(), partitionKey.copy()));
    index.put(key, file, pos);
  }

  private BaseDVFileWriter dvWriter() {
    if (dvWriter == null) {
      this.dvWriter = new BaseDVFileWriter(fileFactory, this::loadPreviousDeletes);
    }

    return dvWriter;
  }

  private PositionDeleteIndex loadPreviousDeletes(String dataFilePath) {
    DeleteFile previous = index.dv(dataFilePath);
    if (previous == null) {
      return null;
    }

    return deleteLoader.loadPositionDeletes(ImmutableList.of(previous), dataFilePath);
  }

  @Override
  public Collection<WriteResult> prepareCommit() throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    for (RollingDataWriter<RowData> writer : dataWriters.values()) {
      writer.close();
      dataFiles.addAll(writer.result().dataFiles());
    }

    dataWriters.clear();
    fileRefs.clear();

    WriteResult.Builder result = WriteResult.builder().addDataFiles(dataFiles);
    if (dvWriter != null) {
      dvWriter.close();
      DeleteWriteResult dvs = dvWriter.result();
      dvs.deleteFiles().forEach(index::updateDv);
      result.addDeleteFiles(dvs.deleteFiles()).addReferencedDataFiles(dvs.referencedDataFiles());
      this.dvWriter = null;
    }

    return ImmutableList.of(result.build());
  }

  @Override
  public List<IndexDvWriterState> snapshotState(long checkpointId) {
    return ImmutableList.of(
        new IndexDvWriterState(subtaskIndex, parallelism, index.serialize(specs, keySerializer)));
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public void close() throws Exception {
    for (RollingDataWriter<RowData> writer : dataWriters.values()) {
      writer.close();
    }

    if (dvWriter != null) {
      dvWriter.close();
    }
  }
}
