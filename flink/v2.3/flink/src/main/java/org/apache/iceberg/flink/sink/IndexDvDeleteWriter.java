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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SerializationUtil;

/**
 * POC: turns retired row positions into deletion vectors. Keyed by data file path, so each data
 * file gets one DV per checkpoint, merged with the last DV written for it.
 */
class IndexDvDeleteWriter extends AbstractStreamOperator<FlinkWriteResult>
    implements OneInputStreamOperator<DVPosition, FlinkWriteResult>, BoundedOneInput {

  @VisibleForTesting static final AtomicLong DVS_WRITTEN = new AtomicLong();

  private final TableLoader tableLoader;
  private final String branch;
  private final Long bootstrapSnapshotId;
  private final boolean rebuildOnRestore;

  private transient Table table;
  private transient Map<Integer, PartitionSpec> specs;
  private transient OutputFileFactory fileFactory;
  private transient DeleteLoader deleteLoader;
  private transient ValueState<byte[]> lastDv;
  private transient BaseDVFileWriter dvWriter;
  private transient Map<String, StructLike> partitions;
  private transient boolean inputEnded;

  IndexDvDeleteWriter(
      TableLoader tableLoader, String branch, Long bootstrapSnapshotId, boolean rebuildOnRestore) {
    this.tableLoader = tableLoader;
    this.branch = branch;
    this.bootstrapSnapshotId = bootstrapSnapshotId;
    this.rebuildOnRestore = rebuildOnRestore;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    TableLoader loader = tableLoader.clone();
    loader.open();
    this.table = loader.loadTable();
    this.specs = table.specs();
    this.fileFactory =
        OutputFileFactory.builderFor(
                table,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getAttemptNumber())
            .format(FileFormat.PUFFIN)
            .build();
    this.deleteLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
    this.partitions = Maps.newHashMap();
    ValueStateDescriptor<byte[]> lastDvDescriptor =
        new ValueStateDescriptor<>(
            "index-dv-last-dv", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
    this.lastDv = context.getKeyedStateStore().getState(lastDvDescriptor);
    if (!context.isRestored()) {
      if (bootstrapSnapshotId != null) {
        bootstrapDvs(table.snapshot(bootstrapSnapshotId));
      }
    } else if (rebuildOnRestore) {
      // The data writer may rebuild its index from the head, so start from the head's DVs too.
      // Safe without a rebuild as well: the committer merges every DV with its file's live DV.
      getKeyedStateBackend()
          .applyToAllKeys(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              lastDvDescriptor,
              (key, state) -> state.clear());
      Snapshot head = table.snapshot(branch);
      if (head != null) {
        bootstrapDvs(head);
      }
    }
  }

  private void bootstrapDvs(Snapshot snapshot) throws IOException {
    int maxParallelism = getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();
    KeyGroupRange ownedKeyGroups =
        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism,
            getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), specs)) {
        for (DeleteFile file : reader) {
          if (ContentFileUtil.isDV(file)
              && ownedKeyGroups.contains(
                  KeyGroupRangeAssignment.assignToKeyGroup(
                      file.referencedDataFile(), maxParallelism))) {
            getKeyedStateBackend().setCurrentKey(file.referencedDataFile());
            lastDv.update(SerializationUtil.serializeToBytes(file.copy()));
          }
        }
      }
    }
  }

  @Override
  public void processElement(StreamRecord<DVPosition> element) {
    DVPosition position = element.getValue();
    PartitionSpec spec = specs.get(position.specId());
    StructLike partition =
        partitions.computeIfAbsent(
            position.dataFilePath(),
            path ->
                StructLikeSerializer.decodePartition(position.partition(), spec.partitionType()));
    if (dvWriter == null) {
      this.dvWriter = new BaseDVFileWriter(fileFactory, this::loadPreviousDeletes);
    }

    dvWriter.delete(position.dataFilePath(), position.position(), spec, partition);
  }

  private PositionDeleteIndex loadPreviousDeletes(String dataFilePath) {
    getKeyedStateBackend().setCurrentKey(dataFilePath);
    try {
      byte[] previous = lastDv.value();
      if (previous == null) {
        return null;
      }

      DeleteFile dv = SerializationUtil.deserializeFromBytes(previous);
      return deleteLoader.loadPositionDeletes(ImmutableList.of(dv), dataFilePath);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    flush(checkpointId);
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

    WriteResult.Builder result = WriteResult.builder();
    if (dvWriter != null) {
      dvWriter.close();
      DeleteWriteResult dvs = dvWriter.result();
      for (DeleteFile dv : dvs.deleteFiles()) {
        getKeyedStateBackend().setCurrentKey(dv.referencedDataFile());
        lastDv.update(SerializationUtil.serializeToBytes(dv));
      }

      DVS_WRITTEN.addAndGet(dvs.deleteFiles().size());
      result.addDeleteFiles(dvs.deleteFiles()).addReferencedDataFiles(dvs.referencedDataFiles());
      this.dvWriter = null;
      partitions.clear();
    }

    output.collect(new StreamRecord<>(new FlinkWriteResult(checkpointId, result.build())));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (dvWriter != null) {
      dvWriter.close();
    }
  }
}
