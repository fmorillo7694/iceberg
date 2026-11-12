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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * POC: records that an operator in front of a SinkV2 writer emits from {@code
 * prepareSnapshotPreBarrier(n)} reach the writer before the writer's own pre-barrier commit for
 * checkpoint n.
 */
@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 120)
class TestPreWriteBarrierOrdering {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  private static final int PARALLELISM = 2;
  private static final int RECORDS = 400_000;

  /**
   * Writer subtask -> (checkpoint id snapshotted -> checkpoint ids of the markers it committed).
   */
  private static final Map<Integer, Map<Long, List<Long>>> COMMITTED = Maps.newConcurrentMap();

  private static final AtomicInteger EMITTED_MARKERS = new AtomicInteger();
  private static final long END_OF_INPUT = -1L;

  @Parameter(index = 0)
  private String topology;

  @Parameter(index = 1)
  private boolean unaligned;

  @Parameters(name = "topology={0}, unaligned={1}")
  static Object[][] parameters() {
    return new Object[][] {
      {"chained", false},
      {"unchained", false},
      {"union", false},
      {"chained", true},
      {"unchained", true},
      {"union", true},
    };
  }

  @TestTemplate
  void preBarrierRecordsReachWriterInTheSameCheckpoint() throws Exception {
    COMMITTED.clear();
    EMITTED_MARKERS.set(0);
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(50L)
            .setParallelism(PARALLELISM);
    if (unaligned) {
      env.getCheckpointConfig().enableUnalignedCheckpoints();
      env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ZERO);
    }

    DataStream<Integer> source = env.addSource(new Numbers()).setParallelism(1);
    TypeInformation<Tuple2<Long, Integer>> markerType = Types.TUPLE(Types.LONG, Types.INT);
    SingleOutputStreamOperator<Tuple2<Long, Integer>> markers =
        source.keyBy(i -> i % 97).transform("pre-barrier-emitter", markerType, new Emitter());
    DataStream<Tuple2<Long, Integer>> writerInput = markers;
    if ("union".equals(topology)) {
      writerInput =
          markers.union(
              markers.keyBy(m -> m.f1).transform("second-emitter", markerType, new Relay()));
    }

    DataStreamSink<Tuple2<Long, Integer>> sink = writerInput.sinkTo(new RecordingSink());
    if ("unchained".equals(topology)) {
      sink.disableChaining();
    }

    env.execute("pre-barrier ordering " + topology);

    int lagging = 0;
    int committed = 0;
    for (Map<Long, List<Long>> perCheckpoint : COMMITTED.values()) {
      for (Map.Entry<Long, List<Long>> entry : perCheckpoint.entrySet()) {
        for (long markerCheckpoint : entry.getValue()) {
          committed++;
          if (markerCheckpoint != END_OF_INPUT && markerCheckpoint != entry.getKey()) {
            lagging++;
          }
        }
      }
    }

    System.out.printf(
        "ORDERING topology=%s unaligned=%s emitted=%d committedBySnapshot=%d lagging=%d%n",
        topology, unaligned, EMITTED_MARKERS.get(), committed, lagging);
    if (!unaligned) {
      assertThat(lagging).isZero();
    }
  }

  /** Emits one marker per subtask from prepareSnapshotPreBarrier, tagged with the checkpoint id. */
  private static class Emitter extends AbstractStreamOperator<Tuple2<Long, Integer>>
      implements OneInputStreamOperator<Integer, Tuple2<Long, Integer>>, BoundedOneInput {
    private boolean inputEnded = false;

    @Override
    public void processElement(StreamRecord<Integer> element) {}

    @Override
    public void endInput() {
      // Like the index-dv writers: the last results go out with end of input, never after it.
      output.collect(
          new StreamRecord<>(
              Tuple2.of(END_OF_INPUT, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())));
      this.inputEnded = true;
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
      super.prepareSnapshotPreBarrier(checkpointId);
      if (inputEnded) {
        return;
      }

      EMITTED_MARKERS.incrementAndGet();
      output.collect(
          new StreamRecord<>(
              Tuple2.of(checkpointId, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())));
    }
  }

  /** Re-emits markers it received before its own barrier, like the DV writer in the POC. */
  private static class Relay extends AbstractStreamOperator<Tuple2<Long, Integer>>
      implements OneInputStreamOperator<Tuple2<Long, Integer>, Tuple2<Long, Integer>>,
          BoundedOneInput {
    private final List<Tuple2<Long, Integer>> pending = Lists.newArrayList();
    private boolean inputEnded = false;

    @Override
    public void endInput() {
      pending.forEach(marker -> output.collect(new StreamRecord<>(marker)));
      pending.clear();
      this.inputEnded = true;
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Long, Integer>> element) {
      pending.add(element.getValue());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
      super.prepareSnapshotPreBarrier(checkpointId);
      if (inputEnded) {
        return;
      }

      for (Tuple2<Long, Integer> marker : pending) {
        output.collect(new StreamRecord<>(marker));
      }

      pending.clear();
    }
  }

  private static class Numbers implements SourceFunction<Integer> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
      for (int i = 0; i < RECORDS && running; i++) {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(i);
        }

        if (i % 200 == 0) {
          Thread.sleep(1);
        }
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }
  }

  /** Writer that records, per snapshotted checkpoint, which marker checkpoints it committed. */
  private static class RecordingSink
      implements Sink<Tuple2<Long, Integer>>, SupportsWriterState<Tuple2<Long, Integer>, Integer> {
    @Override
    public SinkWriter<Tuple2<Long, Integer>> createWriter(WriterInitContext context) {
      return new RecordingWriter(context.getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public StatefulSinkWriter<Tuple2<Long, Integer>, Integer> restoreWriter(
        WriterInitContext context, Collection<Integer> recoveredState) {
      return new RecordingWriter(context.getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public SimpleVersionedSerializer<Integer> getWriterStateSerializer() {
      return new SimpleVersionedSerializer<>() {
        @Override
        public int getVersion() {
          return 1;
        }

        @Override
        public byte[] serialize(Integer obj) {
          return new byte[] {obj.byteValue()};
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) {
          return (int) serialized[0];
        }
      };
    }
  }

  private static class RecordingWriter
      implements StatefulSinkWriter<Tuple2<Long, Integer>, Integer> {
    private final int subtask;
    private final List<Long> sinceLastFlush = Lists.newArrayList();
    private List<Long> flushed = ImmutableList.of();

    RecordingWriter(int subtask) {
      this.subtask = subtask;
    }

    @Override
    public void write(Tuple2<Long, Integer> element, Context context) {
      sinceLastFlush.add(element.f0);
    }

    @Override
    public void flush(boolean endOfInput) {
      // The SinkWriterOperator calls flush right before it emits committables for a checkpoint.
      this.flushed = Lists.newArrayList(sinceLastFlush);
      sinceLastFlush.clear();
    }

    @Override
    public List<Integer> snapshotState(long checkpointId) throws IOException {
      COMMITTED.computeIfAbsent(subtask, s -> Maps.newConcurrentMap()).put(checkpointId, flushed);
      this.flushed = ImmutableList.of();
      return ImmutableList.of(subtask);
    }

    @Override
    public void close() {}
  }
}
