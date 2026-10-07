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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/** Regression tests for JDBC drivers that discard the batch after a failed insert. */
public class ClickHouseBatchFailureTest {

    @Test
    public void failedCheckpointCannotRetryAnEmptyBatch() throws Exception {
        ClearingStatement jdbc = new ClearingStatement();
        ClickHouseBatchOutputFormat format = openFormat(jdbc, 100);
        ClickHouseRowDataSinkFunction sink = new ClickHouseRowDataSinkFunction(format);
        try {
            sink.invoke(GenericRowData.of(1), null);
            sink.invoke(GenericRowData.of(2), null);
            jdbc.failNext = true;

            IOException failure = assertThrows(IOException.class, () -> sink.snapshotState(null));
            assertSame(jdbc.failure, failure.getCause());
            assertEquals(0, jdbc.pendingRows);
            assertEquals(0, jdbc.insertedRows);

            // A later checkpoint must not accept the now-empty JDBC batch as successful.
            IOException nextFailure =
                    assertThrows(IOException.class, () -> sink.snapshotState(null));
            assertSame(jdbc.failure, nextFailure.getCause());
            assertEquals(1, jdbc.executeCalls);
        } finally {
            format.close();
        }
    }

    @Test
    public void failedSizeTriggeredFlushAlsoBlocksCheckpoint() throws Exception {
        ClearingStatement jdbc = new ClearingStatement();
        ClickHouseBatchOutputFormat format = openFormat(jdbc, 1);
        ClickHouseRowDataSinkFunction sink = new ClickHouseRowDataSinkFunction(format);
        try {
            jdbc.failNext = true;
            assertThrows(IOException.class, () -> sink.invoke(GenericRowData.of(1), null));
            assertThrows(IOException.class, () -> sink.snapshotState(null));
            assertEquals(1, jdbc.executeCalls);
            assertEquals(0, jdbc.insertedRows);
        } finally {
            format.close();
        }
    }

    @Test
    public void successfulCheckpointsFlushSuccessiveBatches() throws Exception {
        ClearingStatement jdbc = new ClearingStatement();
        ClickHouseBatchOutputFormat format = openFormat(jdbc, 100);
        ClickHouseRowDataSinkFunction sink = new ClickHouseRowDataSinkFunction(format);
        try {
            sink.invoke(GenericRowData.of(1), null);
            sink.snapshotState(null);
            sink.invoke(GenericRowData.of(2), null);
            sink.snapshotState(null);
            sink.snapshotState(null);
            assertEquals(2, jdbc.executeCalls);
            assertEquals(2, jdbc.insertedRows);
        } finally {
            format.close();
        }
    }

    private static ClickHouseBatchOutputFormat openFormat(ClearingStatement jdbc, int batchSize)
            throws Exception {
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withUrl("jdbc:clickhouse://localhost:8123")
                        .withDatabaseName("test")
                        .withTableName("events")
                        .withBatchSize(batchSize)
                        .withFlushInterval(Duration.ofHours(1))
                        .withMaxRetries(5)
                        .build();
        ClickHousePreparedStatement statement = proxy(ClickHousePreparedStatement.class, jdbc);
        ClickHouseConnection connection =
                proxy(
                        ClickHouseConnection.class,
                        (obj, method, args) -> {
                            if (method.getName().equals("prepareStatement")) {
                                return statement;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
        ClickHouseConnectionProvider provider =
                new ClickHouseConnectionProvider(options) {
                    @Override
                    public synchronized ClickHouseConnection getOrCreateConnection() {
                        return connection;
                    }
                };
        ClickHouseBatchOutputFormat format =
                new ClickHouseBatchOutputFormat(
                        provider,
                        new String[] {"id"},
                        new String[0],
                        new String[0],
                        new LogicalType[] {new IntType()},
                        options);
        format.setRuntimeContext(
                proxy(
                        RuntimeContext.class,
                        (obj, method, args) -> {
                            throw new UnsupportedOperationException(method.getName());
                        }));
        format.open(0, 1);
        return format;
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static class ClearingStatement implements InvocationHandler {
        private final SQLException failure =
                new SQLException("The target server failed to respond");
        private boolean failNext;
        private int pendingRows;
        private int insertedRows;
        private int executeCalls;

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args)
                throws SQLException {
            switch (method.getName()) {
                case "setInt":
                case "close":
                    return null;
                case "addBatch":
                    pendingRows++;
                    return null;
                case "executeBatch":
                    executeCalls++;
                    int rows = pendingRows;
                    // InputBasedPreparedStatement clears its batch on both success and failure.
                    pendingRows = 0;
                    if (failNext) {
                        failNext = false;
                        throw failure;
                    }
                    insertedRows += rows;
                    int[] results = new int[rows];
                    Arrays.fill(results, 1);
                    return results;
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        }
    }
}
