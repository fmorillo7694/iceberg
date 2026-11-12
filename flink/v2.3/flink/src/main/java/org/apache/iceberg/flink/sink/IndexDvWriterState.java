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
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** POC: the key position index of one writer subtask, captured at a checkpoint. */
final class IndexDvWriterState {
  private final int subtaskIndex;
  private final int parallelism;
  private final byte[] index;

  IndexDvWriterState(int subtaskIndex, int parallelism, byte[] index) {
    this.subtaskIndex = subtaskIndex;
    this.parallelism = parallelism;
    this.index = index;
  }

  int subtaskIndex() {
    return subtaskIndex;
  }

  int parallelism() {
    return parallelism;
  }

  byte[] index() {
    return index;
  }

  static class Serializer implements SimpleVersionedSerializer<IndexDvWriterState> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    public byte[] serialize(IndexDvWriterState state) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeInt(state.subtaskIndex());
        out.writeInt(state.parallelism());
        out.writeInt(state.index().length);
        out.write(state.index());
      }

      return bytes.toByteArray();
    }

    @Override
    public IndexDvWriterState deserialize(int version, byte[] serialized) throws IOException {
      if (version != VERSION) {
        throw new IOException("Unrecognized index-dv writer state version: " + version);
      }

      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
        int subtaskIndex = in.readInt();
        int parallelism = in.readInt();
        byte[] index = new byte[in.readInt()];
        in.readFully(index);
        return new IndexDvWriterState(subtaskIndex, parallelism, index);
      }
    }
  }
}
