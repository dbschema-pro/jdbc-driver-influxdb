package com.wisecoders.jdbc.influxdb

import com.influxdb.client.QueryApi
import com.wisecoders.common_jdbc.jvm.result_set.ArrayResultSet
import com.wisecoders.common_jdbc.jvm.sql.AbstractDatabaseMetaData
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class InfluxMetaData(private val influxConnection: InfluxConnection) : AbstractDatabaseMetaData() {

    override fun getSchemas(): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(listOf("TABLE_SCHEMA", "TABLE_CAT"))
        try {
            for (bucket in influxConnection.client.bucketsApi.findBuckets()) {
                result.addRow(listOf(bucket.name.toString(), null))
            }
        } catch (ex: Throwable) {
            for (fluxTable in influxConnection.client.queryApi.query("buckets()")) {
                for (fluxRecord in fluxTable.records) {
                    result.addRow(listOf(fluxRecord.getValueByKey("name").toString(), null))
                }
            }
        }
        return result
    }


    override fun getCatalogs(): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(listOf("TABLE_SCHEMA", "TABLE_CAT"))
        for (organization in influxConnection.client.organizationsApi.findOrganizations()) {
            result.addRow(listOf(organization.name.toString()))
        }
        return result
    }


    /**
     * @see java.sql.DatabaseMetaData.getTables
     */
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEMA", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
            )
        )

        result.setColumnNames(listOf("TABLE_CAT"))
        for (fluxTable in influxConnection.client.queryApi.query(
            "import \"influxdata/influxdb/schema\"\n\n  schema.measurements(bucket: \"$schemaPattern\")"
        )) {
            for (fluxRecord in fluxTable.records) {
                result.addRow(createTableRow(catalog, fluxRecord.getValueByKey("_value").toString(), null))
            }
        }
        return result
    }

    private fun createTableRow(
        catalogName: String?,
        tableName: String,
        comment: String?,
    ): List<String?> {
        val data = listOf(
            catalogName, // TABLE_CAT
            "", // TABLE_SCHEMA
            tableName, // TABLE_NAME
            "TABLE", // TABLE_TYPE
            comment, // REMARKS
            "", // TYPE_CAT
            "", // TYPE_SCHEM
            "", // TYPE_NAME
            "", // SELF_REFERENCING_COL_NAME
            "" // REF_GENERATION
        )
        return data
    }

    /**
     * @see java.sql.DatabaseMetaData.getColumns
     */
    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS"
            )
        )

        val fluxQuery = """
             import "influxdata/influxdb/schema"
             schema.measurementFieldKeys(bucket: "$schemaPattern", measurement: "$tableNamePattern", start: ${influxConnection.startDays}d )
             """.trimIndent()
        for (columnNames in influxConnection.client.queryApi.query(fluxQuery)) {
            for (columnNamesRecord in columnNames.records) {
                val columnName = columnNamesRecord.getValueByKey("_value").toString()
                if (!columnName.startsWith("_")) {
                    val columnDataType =
                        getColumnDataType(influxConnection.client.queryApi, schemaPattern, tableNamePattern, columnName)
                    addColumn(catalog, tableNamePattern, result, columnName, columnDataType)
                }
            }
        }
        val fluxQuery2 = """import "influxdata/influxdb/schema"
schema.measurementTagKeys(bucket: "$schemaPattern", measurement: "$tableNamePattern" )"""
        for (columnNames in influxConnection.client.queryApi.query(fluxQuery2)) {
            for (columnNamesRecord in columnNames.records) {
                val columnName = columnNamesRecord.getValueByKey("_value").toString()
                if (!columnName.startsWith("_")) {
                    addColumn(catalog, tableNamePattern, result, columnName, "string")
                }
            }
        }
        return result
    }

    private fun addColumn(
        catalogName: String?,
        tableName: String?,
        result: ArrayResultSet,
        columnName: String,
        columnDataType: String?,
    ) {
        result.addRow(
            listOf(
                catalogName,  // "TABLE_CAT",
                null,  // "TABLE_SCHEMA",
                tableName,  // "TABLE_NAME", (i.e. Cassandra Collection Name)
                columnName,  // "COLUMN_NAME",
                "4",  // "DATA_TYPE",
                columnDataType,  // "TYPE_NAME", -- I LET THIS INTENTIONALLY TO USE .toString() BECAUSE OF USER DEFINED TYPES.
                "800",  // "COLUMN_SIZE",
                "0",  // "BUFFER_LENGTH", (not used)
                "0",  // "DECIMAL_DIGITS",
                "10",  // "NUM_PREC_RADIX",
                "0",  // "NULLABLE", // I RETREIVE HERE IF IS FROZEN ( MANDATORY ) OR NOT ( NULLABLE )
                "",  // "REMARKS",
                "",  // "COLUMN_DEF",
                "0",  // "SQL_DATA_TYPE", (not used)
                "0",  // "SQL_DATETIME_SUB", (not used)
                "800",  // "CHAR_OCTET_LENGTH",
                "1",  // "ORDINAL_POSITION",
                "NO",  // "IS_NULLABLE",
                null,  // "SCOPE_CATLOG", (not a REF type)
                null,  // "SCOPE_SCHEMA", (not a REF type)
                null,  // "SCOPE_TABLE", (not a REF type)
                null,  // "SOURCE_DATA_TYPE", (not a DISTINCT or REF type)
                "NO",  // "IS_AUTOINCREMENT" (can be auto-generated, but can also be specified)
                null // TABLE_OPTIONS
            )
        )
    }


    override fun getPrimaryKeys(
        catalog: String?,
        schema: String?,
        table: String?,
    ): ResultSet {
        val result = ArrayResultSet()
        val fluxQuery2 = """import "influxdata/influxdb/schema"
schema.measurementTagKeys(bucket: "$schema", measurement: "$table" )"""
        var seq = 0
        for (columnNames in influxConnection.client.queryApi.query(fluxQuery2)) {
            for (columnNamesRecord in columnNames.records) {
                val columnName = columnNamesRecord.getValueByKey("_value").toString()
                if (!columnName.startsWith("_")) {
                    result.addRow(
                        listOf(
                            catalog,  // "TABLE_CAT",
                            schema,  // "TABLE_SCHEMA",
                            table,  // "TABLE_NAME", (i.e. Cassandra Collection Name)
                            columnName,  // "COLUMN_NAME",
                            "" + (++seq),
                            "PK_$table"
                        )
                    )
                }
            }
        }
        return result
    }

    @Throws(SQLException::class)
    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"
            )
        )
        return result
    }


    /*
    @Override
    public ResultSet getIndexInfo(String catalogName, String schemaName, String tableName, boolean unique, boolean approximate) {
        ArrayResultSet result = new ArrayResultSet();
        result.setColumnNames(new String[]{"TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"});

        String fluxQuery = "import \"influxdata/influxdb/schema\"\n\n  schema.measurementTagKeys(bucket: \"" + schemaName + "\", measurement: \"" + tableName + "\")";
        for (FluxTable fluxTable : influxConnection.client.getQueryApi().query( fluxQuery)) {
            for (FluxRecord fluxRecord : fluxTable.getRecords()) {
                String columnName = String.valueOf( fluxRecord.getValueByKey("_value"));
                if ( !columnName.startsWith("_") ) {
                    result.addRow(new String[]{catalogName, // "TABLE_CAT",
                            null, // "TABLE_SCHEMA",
                            fluxTable.toString(), // "TABLE_NAME", (measurement)
                            "YES", // "NON-UNIQUE",
                            columnName, // "INDEX QUALIFIER",
                            columnName, // "INDEX_NAME",
                            "0", // "TYPE",
                            "0", // "ORDINAL_POSITION"
                            columnName, // "COLUMN_NAME",
                            "A", // "ASC_OR_DESC",
                            "0", // "CARDINALITY",
                            "0", // "PAGES",
                            "" // "FILTER_CONDITION",
                    });
                }
            }
        }
        return result;
    }*/


    companion object {
        fun getColumnDataType(
            queryApi: QueryApi,
            schemaName: String?,
            measurement: String?,
            columnName: String,
        ): String? {
            val fluxToGetDataType = """
                from(bucket: "$schemaName") 
                |> range(start: -40d) 
                |> filter(fn: (r) => r._measurement == "$measurement") 
                |> filter(fn: (r) => r._field == "$columnName") 
                |> keep(columns: ["_value"]) 
                |> last() 
                
                """.trimIndent()

            val tableList = queryApi.query(fluxToGetDataType)
            if (tableList.size > 0) {
                val columnTypeTable = tableList[0]

                for (columnType in columnTypeTable.columns) {
                    if (columnType.label == "_value") {
                        return columnType.dataType
                    }
                }
            }
            return null
        }
    }
}
