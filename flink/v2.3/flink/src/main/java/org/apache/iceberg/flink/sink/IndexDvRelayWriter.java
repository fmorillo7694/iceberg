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
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * POC: SinkV2 writer of the index-dv mode. The files are written in the pre-write topology, so this
 * writer only collects the results that reach it before a checkpoint barrier and hands them to the
 * committer for that checkpoint.
 */
class IndexDvRelayWriter implements CommittingSinkWriter<RowData, WriteResult> {
  static final InternalTypeInfo<RowData> RELAY_TYPE =
      InternalTypeInfo.of(RowType.of(new VarBinaryType(VarBinaryType.MAX_LENGTH)));

  private final List<WriteResult> pending = Lists.newArrayList();

  @Override
  public void write(RowData element, Context context) {
    try {
      pending.add(
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              new WriteResultSerializer(), element.getBinary(0)));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode an index-dv write result", e);
    }
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public Collection<WriteResult> prepareCommit() {
    if (pending.isEmpty()) {
      return ImmutableList.of();
    }

    WriteResult merged = WriteResult.builder().addAll(pending).build();
    pending.clear();
    return ImmutableList.of(merged);
  }

  @Override
  public void close() {}

  /** Encodes a write result of the pre-write topology as a single-column row for the relay. */
  static class Encoder implements MapFunction<FlinkWriteResult, RowData> {
    @Override
    public RowData map(FlinkWriteResult result) throws IOException {
      return GenericRowData.of(
          (Object)
              SimpleVersionedSerialization.writeVersionAndSerialize(
                  new WriteResultSerializer(), result.writeResult()));
    }
  }
}
