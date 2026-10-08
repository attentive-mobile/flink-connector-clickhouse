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

package org.apache.flink.connector.clickhouse.internal.connection;

import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

/** Wrapper class for ClickHousePreparedStatement. */
public class ClickHouseStatementWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseStatementWrapper.class);
    public final ClickHousePreparedStatement statement;

    private final String sqlTemplate;

    // Counts addBatch calls, not the driver's internal queue after an execution failure.
    private int batchSize;

    private String failureId;

    public ClickHouseStatementWrapper(ClickHousePreparedStatement statement) {
        this(statement, null);
    }

    public ClickHouseStatementWrapper(ClickHousePreparedStatement statement, String sqlTemplate) {
        this.statement = statement;
        this.sqlTemplate = sqlTemplate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void addBatch() throws SQLException {
        statement.addBatch();
        batchSize++;
    }

    public int[] executeBatch() throws SQLException {
        return executeBatch(0, 0, batchSize);
    }

    public int[] executeBatch(int retryTimes, int maxRetries, int expectedRows)
            throws SQLException {
        if (retryTimes == 0) {
            failureId = null;
        }
        final int rowsAdded = batchSize;
        final long startNanos = System.nanoTime();
        try {
            if (retryTimes > 0) {
                LOG.warn(
                        "ClickHouse executeBatch retry starting, retry times = {}, max_retries = {}, batch_id = {}, expected_rows = {}, rows_added_since_last_execute = {}, sql_template = {}",
                        retryTimes,
                        maxRetries,
                        failureId,
                        expectedRows,
                        rowsAdded,
                        sqlTemplate);
            }
            int[] updateCounts = statement.executeBatch();
            if (retryTimes > 0) {
                LOG.warn(
                        "ClickHouse executeBatch retry returned, retry times = {}, max_retries = {}, batch_id = {}, expected_rows = {}, rows_added_since_last_execute = {}, returned_update_count = {}, duration_ms = {}, sql_template = {}",
                        retryTimes,
                        maxRetries,
                        failureId,
                        expectedRows,
                        rowsAdded,
                        updateCounts.length,
                        (System.nanoTime() - startNanos) / 1_000_000L,
                        sqlTemplate);
            }
            return updateCounts;
        } catch (Exception exception) {
            if (failureId == null) {
                failureId = UUID.randomUUID().toString();
            }
            SQLException sqlException =
                    exception instanceof SQLException ? (SQLException) exception : null;
            int[] exceptionUpdateCounts =
                    exception instanceof BatchUpdateException
                            ? ((BatchUpdateException) exception).getUpdateCounts()
                            : null;
            LOG.error(
                    "ClickHouse executeBatch error, retry times = {}, max_retries = {}, connector_retries_exhausted = {}, batch_id = {}, expected_rows = {}, rows_added_since_last_execute = {}, duration_ms = {}, sql_state = {}, error_code = {}, exception_update_count = {}, sql_template = {}",
                    retryTimes,
                    maxRetries,
                    retryTimes >= maxRetries,
                    failureId,
                    expectedRows,
                    rowsAdded,
                    (System.nanoTime() - startNanos) / 1_000_000L,
                    sqlException == null ? null : sqlException.getSQLState(),
                    sqlException == null ? null : sqlException.getErrorCode(),
                    exceptionUpdateCounts == null ? -1 : exceptionUpdateCounts.length,
                    sqlTemplate,
                    exception);
            throw exception;
        } finally {
            batchSize = 0;
        }
    }

    public void close() throws SQLException {
        statement.close();
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        statement.setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        statement.setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        statement.setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        statement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        statement.setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        statement.setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        statement.setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        statement.setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        statement.setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        statement.setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        statement.setDate(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        statement.setTimestamp(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        statement.setArray(parameterIndex, new ObjectArray(array));
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        statement.setObject(parameterIndex, x);
    }

    public void clearParameters() throws SQLException {
        statement.clearParameters();
    }

    public ResultSet executeQuery() throws SQLException {
        return statement.executeQuery();
    }
}
