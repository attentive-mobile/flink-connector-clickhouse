/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;

import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Runs a local Flink job to verify restart and replay after a destructive JDBC failure. */
public class ClickHouseBatchRecoveryITCase {
    private static final List<Integer> INSERTED = new CopyOnWriteArrayList<>();
    private static final List<Integer> RESTORED = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean FAIL_ONCE = new AtomicBoolean();
    private static final AtomicInteger OPENS = new AtomicInteger();

    @Test(timeout = 120000)
    public void sizeTriggeredFailureRestartsAndReplays() throws Exception {
        verifyRecovery(1);
    }

    @Test(timeout = 120000)
    public void checkpointFailureRestartsAndReplays() throws Exception {
        verifyRecovery(100);
    }

    private void verifyRecovery(int batchSize) throws Exception {
        INSERTED.clear();
        RESTORED.clear();
        FAIL_ONCE.set(true);
        OPENS.set(0);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.enableCheckpointing(200, CheckpointingMode.AT_LEAST_ONCE);
        // A synchronous sink exception must fail the task, not just exhaust this allowance.
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // Match the segmentation job's explicit restart strategy, including the initial delay.
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.seconds(30), Time.minutes(3), 1.5, Time.hours(1), 0.1));
        env.addSource(new ReplaySource()).addSink(new ProbeSink(batchSize));
        env.execute("clickhouse batch recovery");
        assertEquals(Arrays.asList(0, 1), INSERTED);
        assertTrue(RESTORED.contains(1));
        assertEquals(2, OPENS.get());
    }

    private static class ReplaySource extends RichParallelSourceFunction<Integer>
            implements ListCheckpointed<Integer>, CheckpointListener {
        private volatile boolean running = true;
        private volatile boolean checkpointComplete;
        private int next;

        @Override
        public void run(SourceContext<Integer> context) throws Exception {
            if (next == 0) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(next++);
                }
            }
            // Establish a completed checkpoint before the row whose insert will fail.
            while (running && !checkpointComplete) {
                Thread.sleep(10);
            }
            if (running && next == 1) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(next++);
                }
            }
            while (running && !INSERTED.contains(1)) {
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(next);
        }

        @Override
        public void restoreState(List<Integer> state) {
            next = state.get(0);
            RESTORED.add(next);
            checkpointComplete = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointComplete = true;
        }
    }

    private static class ProbeSink extends RichSinkFunction<Integer>
            implements CheckpointedFunction {
        private final int batchSize;
        private transient ClickHouseBatchOutputFormat format;

        private ProbeSink(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            OPENS.incrementAndGet();
            List<Integer> pending = new ArrayList<>();
            int[] value = new int[1];
            format =
                    ClickHouseBatchFailureTest.openFormat(
                            (proxy, method, args) -> {
                                switch (method.getName()) {
                                    case "setInt":
                                        value[0] = (Integer) args[1];
                                        return null;
                                    case "addBatch":
                                        pending.add(value[0]);
                                        return null;
                                    case "executeBatch":
                                        List<Integer> rows = new ArrayList<>(pending);
                                        pending.clear();
                                        if (rows.contains(1)
                                                && FAIL_ONCE.compareAndSet(true, false)) {
                                            throw new SQLException(
                                                    "The target server failed to respond");
                                        }
                                        INSERTED.addAll(rows);
                                        int[] results = new int[rows.size()];
                                        Arrays.fill(results, 1);
                                        return results;
                                    case "close":
                                        return null;
                                    default:
                                        throw new UnsupportedOperationException(method.getName());
                                }
                            },
                            batchSize);
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            format.writeRecord(GenericRowData.of(value));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            format.flush();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void close() {
            if (format != null) {
                format.close();
            }
        }
    }
}
