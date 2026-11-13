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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * POC: hands the write results of the dynamic index-dv writers to the sink's aggregator, one per
 * table, branch and spec for each checkpoint. Only the writer is used, as with the forward writer.
 */
class DynamicIndexDvRelaySink implements Sink<byte[]>, SupportsCommitter<DynamicWriteResult> {
  @Override
  public SinkWriter<byte[]> createWriter(WriterInitContext context) {
    return new RelayWriter();
  }

  @Override
  public Committer<DynamicWriteResult> createCommitter(CommitterInitContext context) {
    throw new UnsupportedOperationException(
        "The index-dv relay only writes; committing is handled by the main sink");
  }

  @Override
  public SimpleVersionedSerializer<DynamicWriteResult> getCommittableSerializer() {
    return new DynamicWriteResultSerializer();
  }

  private static class RelayWriter implements CommittingSinkWriter<byte[], DynamicWriteResult> {
    private final Map<TableKey, Map<Integer, WriteResult.Builder>> pending = Maps.newHashMap();

    @Override
    public void write(byte[] element, Context context) {
      DynamicWriteResult result;
      try {
        result =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                new DynamicWriteResultSerializer(), element);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to decode an index-dv write result", e);
      }

      pending
          .computeIfAbsent(result.key(), key -> Maps.newHashMap())
          .computeIfAbsent(result.specId(), specId -> WriteResult.builder())
          .add(result.writeResult());
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public Collection<DynamicWriteResult> prepareCommit() {
      List<DynamicWriteResult> results = Lists.newArrayList();
      for (Map.Entry<TableKey, Map<Integer, WriteResult.Builder>> table : pending.entrySet()) {
        for (Map.Entry<Integer, WriteResult.Builder> spec : table.getValue().entrySet()) {
          results.add(
              new DynamicWriteResult(table.getKey(), spec.getKey(), spec.getValue().build()));
        }
      }

      pending.clear();
      return results;
    }

    @Override
    public void close() {}
  }
}
