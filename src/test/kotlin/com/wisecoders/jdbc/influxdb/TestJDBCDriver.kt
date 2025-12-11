package com.wisecoders.jdbc.influxdb

import com.wisecoders.common_jdbc.jvm.sql.printResultSet
import java.io.IOException
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled("disabled until we configure it to start the docker InfluxDB container")
class TestJDBCDriver {
    private var con: Connection? = null

    @BeforeEach
    @Throws(ClassNotFoundException::class, SQLException::class, IOException::class)
    fun setUp() {
        Class.forName("com.wisecoders.jdbc.influxdb.JdbcDriver")

        val url = "http://localhost:8086?" + "token=" + URLEncoder.encode(
            InfluxTokenHolder.INFLUX_TOKEN,
            StandardCharsets.UTF_8
        ) + "&org=" + "dbschema"
        con = DriverManager.getConnection(url, null, null)
    }

    @Test
    @Throws(Exception::class)
    fun testListDatabases() {
        val stmt = con!!.createStatement()
        stmt.executeQuery("list organizations").printResultSet()
        stmt.close()

        val stmt1 = con!!.createStatement()
        stmt1.executeQuery("buckets()").printResultSet()
        stmt1.close()

        val stmt2 = con!!.createStatement()
        stmt2.executeQuery(" import \"influxdata/influxdb/schema\"\n\n  schema.measurements(bucket: \"sample\")")
            .printResultSet()
        stmt2.close()

        val stmt3 = con!!.createStatement()
        stmt3.executeQuery("import \"influxdata/influxdb/schema\"\n\n  schema.fieldKeys(bucket: \"sample\")")
            .printResultSet()
        stmt3.close()
    }

    @Test
    @Throws(Exception::class)
    fun testDatabaseMetaData() {
        val schemas: MutableList<String> = ArrayList()
        val rs = con!!.metaData.schemas
        while (rs.next()) {
            val schemaName = rs.getString(1)
            schemas.add(schemaName)
            println("Schema $schemaName")
        }
        rs.close()

        for (schema in schemas) {
            println("Schema $schema")
            val rsTables = con!!.metaData.getTables(null, schema, null, arrayOf("TABLE"))
            val tables: MutableList<String> = ArrayList()
            while (rsTables.next()) {
                val tableName = rsTables.getString(3)
                tables.add(tableName)
            }
            rsTables.close()

            for (table in tables) {
                println("  Measurement $table")
                val rsColumns = con!!.metaData.getColumns(null, schema, table, null)
                while (rsColumns.next()) {
                    println("   Column " + rsColumns.getString(4) + " type " + rsColumns.getString(6))
                }

                val rsIndexes = con!!.metaData.getIndexInfo(null, schema, table, false, true)
                while (rsIndexes.next()) {
                    println("   Index " + rsIndexes.getString(6) + " Column " + rsIndexes.getString(9))
                }
            }
        }
    }


    @Test
    @Throws(Exception::class)
    fun testQuery() {
        val flux = "from(bucket:\"sample\") |> range(start: 0)"
        val statement = con!!.createStatement()
        statement.execute(flux)
        val rs = statement.resultSet
        rs.printResultSet()
        statement.close()
    }
}
