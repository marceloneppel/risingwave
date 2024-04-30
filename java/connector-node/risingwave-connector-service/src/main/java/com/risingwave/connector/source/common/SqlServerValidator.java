// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerValidator extends DatabaseValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(SqlServerValidator.class);

    private final Map<String, String> userProps;

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    private final String user;
    private final String dbName;
    private final String schemaName;
    private final String tableName;

    // Whether the properties to validate is shared by multiple tables.
    // If true, we will skip validation check for table
    private final boolean isCdcSourceJob;

    public SqlServerValidator(
            Map<String, String> userProps, TableSchema tableSchema, boolean isCdcSourceJob)
            throws SQLException {
        this.userProps = userProps;
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbPort = userProps.get(DbzConnectorConfig.PORT);
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var user = userProps.get(DbzConnectorConfig.USER);
        var password = userProps.get(DbzConnectorConfig.PASSWORD);
        var jdbcUrl =
                String.format(
                        "jdbc:sqlserver://%s:%s;databaseName=%s;user=%s;password=%s;encrypt=false",
                        dbHost, dbPort, dbName, user, password);

        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);

        this.dbName = dbName;
        this.user = user;
        this.schemaName = userProps.get(DbzConnectorConfig.SQL_SERVER_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.isCdcSourceJob = isCdcSourceJob;
    }

    @Override
    public void validateDbConfig() {
        // TODO: check database server version
    }

    @Override
    public void validateUserPrivilege() {
        try {
            validatePrivileges();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateTable() {
        //        try {
        //            validateTableSchema();
        //        } catch (SQLException e) {
        //            throw ValidatorUtils.internalError(e.getMessage());
        //        }
    }

    @Override
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    /** For Citus which is a distributed version of PG */
    public void validateDistributedTable() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("citus.distributed_table"))) {
            stmt.setString(1, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (!ret.equalsIgnoreCase("distributed")) {
                    throw ValidatorUtils.invalidArgument("Citus table is not a distributed table");
                }
            }
        }
    }

    private void validateTableSchema() throws SQLException {
        if (isCdcSourceJob) {
            return;
        }
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table"))) {
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                    throw ValidatorUtils.invalidArgument("Postgres table or schema doesn't exist");
                }
            }
        }

        // check primary key
        // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.pk"))) {
            stmt.setString(1, this.tableName);
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                // RisingWave always use lower case for column name
                pkFields.add(name.toLowerCase());
            }

            if (!ValidatorUtils.isPrimaryKeyMatch(tableSchema, pkFields)) {
                throw ValidatorUtils.invalidArgument("Primary key mismatch");
            }
        }

        // Check whether source schema match table schema on upstream
        // All columns defined must exist in upstream database
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_schema"))) {
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();

            // Field names in lower case -> data type
            Map<String, String> schema = new HashMap<>();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                schema.put(field.toLowerCase(), dataType);
            }

            for (var e : tableSchema.getColumnTypes().entrySet()) {
                // skip validate internal columns
                if (e.getKey().startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var dataType = schema.get(e.getKey().toLowerCase());
                if (dataType == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + e.getKey() + "' not found in the upstream database");
                }
                if (!isDataTypeCompatible(dataType, e.getValue())) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column " + e.getKey());
                }
            }
        }
    }

    private void validatePrivileges() throws SQLException {}

    private void validateTablePrivileges(boolean isSuperUser) throws SQLException {
        // cdc source job doesn't have table schema to validate, since its schema is fixed to jsonb
        if (isSuperUser || isCdcSourceJob) {
            return;
        }
    }

    protected void alterPublicationIfNeeded() throws SQLException {
        if (isCdcSourceJob) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is created by a shared source unexpectedly");
        }
    }

    @Override
    public void close() throws Exception {
        if (null != jdbcConnection) {
            jdbcConnection.close();
        }
    }

    private boolean isDataTypeCompatible(String pgDataType, Data.DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (pgDataType) {
            case "smallint":
                return Data.DataType.TypeName.INT16_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "integer":
                return Data.DataType.TypeName.INT32_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == Data.DataType.TypeName.INT64_VALUE;
            case "float":
            case "real":
                return val == Data.DataType.TypeName.FLOAT_VALUE
                        || val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "boolean":
                return val == Data.DataType.TypeName.BOOLEAN_VALUE;
            case "double":
            case "double precision":
                return val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
            case "numeric":
                return val == Data.DataType.TypeName.DECIMAL_VALUE
                        // We allow user to map numeric into rw_int256 or varchar to avoid precision
                        // loss in the conversion from pg-numeric to rw-numeric
                        || val == Data.DataType.TypeName.INT256_VALUE
                        || val == Data.DataType.TypeName.VARCHAR_VALUE;
            case "varchar":
            case "character varying":
                return val == Data.DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
    }
}
