package com.wisecoders.jdbc.influxdb

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import java.io.IOException
import java.sql.SQLException
import java.time.Instant
import java.time.Period
import java.util.Properties
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Disabled("disabled until we configure it to start the docker InfluxDB container")
class TestQueryApi {
    private var influxDBClient: InfluxDBClient? = null
    private val prop = Properties()

    init {
        // Read InfluxDb.md how to setup
        prop.setProperty(URL, "http://localhost:8086")
        prop.setProperty(TOKEN, InfluxTokenHolder.INFLUX_TOKEN)
        prop.setProperty(ORG, "dbschema" )
        prop.setProperty(BUCKET, "sample" )
    }

    companion object{
        const val URL = "url"
        const val TOKEN = "token"
        const val ORG = "org"
        const val BUCKET = "bucket"
    }

    @AfterEach
    fun closeConnection() {
        influxDBClient!!.close()
    }

    @BeforeEach
    @Throws(IOException::class)
    fun prepareData() {
        this.influxDBClient = InfluxDBClientFactory.create(
            prop.getProperty(URL),
            prop.getProperty(TOKEN).toCharArray(),
            prop.getProperty(ORG),
            prop.getProperty(BUCKET)
        )

        val writeApi = influxDBClient!!.writeApiBlocking
        // where we save the clients of a shop, and the bill value.
        val today = Instant.now()
        val yesterday = today.minus(Period.ofDays(1))
        val daybefore = yesterday.minus(Period.ofDays(1))


        val pointsToAdd: MutableList<Point> = ArrayList()
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Peter").addTag("lastName", "Smithson")
                .addField("bill", 52.00).time(daybefore, WritePrecision.S)
        )
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Bill").addTag("lastName", "Coulam")
                .addField("bill", 22.45).time(daybefore, WritePrecision.S)
        )
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Peter").addTag("lastName", "Johnson")
                .addField("bill", 57.67).time(yesterday, WritePrecision.S)
        )
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Bill").addTag("lastName", "Coulam")
                .addField("bill", 34.22).time(yesterday, WritePrecision.S)
        )
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Jayne").addTag("lastName", "Johnson")
                .addField("bill", 51.34).time(today, WritePrecision.S)
        )
        pointsToAdd.add(
            Point.measurement("clients").addTag("firstName", "Bill").addTag("lastName", "Coulam")
                .addField("bill", 22.12).time(today, WritePrecision.S)
        )
        writeApi.writePoints(pointsToAdd)


        val pointsToAdd2: MutableList<Point> = ArrayList()
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "west").addField("value", 52.0)
                .time(daybefore, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "north").addField("value", 60.0)
                .time(daybefore, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "south").addField("value", 62.0)
                .time(daybefore, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "west").addField("value", 55.0)
                .time(yesterday, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "north").addField("value", 61.0)
                .time(yesterday, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "south").addField("value", 66.0)
                .time(yesterday, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "west").addField("value", 56.0)
                .time(today, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "north").addField("value", 67.0)
                .time(today, WritePrecision.S)
        )
        pointsToAdd2.add(
            Point.measurement("temperature").addTag("location", "south").addField("value", 63.0)
                .time(today, WritePrecision.S)
        )

        writeApi.writePoints(pointsToAdd2)
    }

    /**
     * This Test function gets the buckets for the given organisation
     * Buckets are Analogous to schemas within an traditional RDBMS.
     * Org is similar in to a database in that its a group of Buckets
     */
    @Test
    @Throws(SQLException::class)
    fun listBuckets() {
        val bucketApi = influxDBClient!!.bucketsApi
        val buckets = bucketApi.findBucketsByOrgName(prop.getProperty(ORG))
        for (bucket in buckets) {
            System.out.printf("Bucket $bucket")
        }
        influxDBClient!!.close()
    }


    /**
     * List measurements (similar with tables) and its columns
     */
    @Test
    @Throws(SQLException::class)
    fun listColumns() {
        val bucket = prop.getProperty(BUCKET)
        val queryApi = influxDBClient!!.queryApi

        val flux = """
             import "influxdata/influxdb/schema"
             
             schema.measurements(bucket: "$bucket")
             """.trimIndent()

        for (fluxTable in queryApi.query(flux)) {
            for (fluxRecord in fluxTable.records) {
                val measurement = fluxRecord.getValueByKey("_value").toString()
                println("#### Measurement $measurement")
                val flux2 = """
                    import "influxdata/influxdb/schema"
                    
                    schema.measurementFieldKeys(bucket: "$bucket", measurement: "$measurement")
                    """.trimIndent()

                for (columnNames in queryApi.query(flux2)) {
                    for (columnNamesRecord in columnNames.records) {
                        val column = columnNamesRecord.getValueByKey("_value").toString()
                        println("  Column $column")

                        val fluxToGetDataType = """
                            from(bucket: "$bucket") 
                            |> range(start: -40d) 
                            |> filter(fn: (r) => r._measurement == "$measurement") 
                            |> filter(fn: (r) => r._field == "$column") 
                            |> keep(columns: ["_value"]) 
                            |> last() 
                            
                            """.trimIndent()

                        println(fluxToGetDataType)

                        val columnTypeTable = queryApi.query(fluxToGetDataType)[0]

                        for (columnType in columnTypeTable.columns) {
                            if (columnType.label == "_value") {
                                println("  ColumnType " + columnType.dataType)
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * List measurements (similar with tables) and its columns
     */
    @Test
    fun listMeasurementAndFieldNames() {
        val bucket = prop.getProperty(BUCKET)
        val queryApi = influxDBClient!!.queryApi

        val flux = """
             import "influxdata/influxdb/schema"
             
             schema.measurements(bucket: "$bucket")
             """.trimIndent()

        for (fluxTable in queryApi.query(flux)) {
            for (fluxRecord in fluxTable.records) {
                val measurement = fluxRecord.getValueByKey("_value").toString()
                println("#### Measurement $measurement")
                val flux2 = """
                    import "influxdata/influxdb/schema"
                    
                    schema.measurementFieldKeys(bucket: "$bucket", measurement: "$measurement")
                    """.trimIndent()

                for (fluxTable2 in queryApi.query(flux2)) {
                    for (fluxRecord2 in fluxTable2.records) {
                        val column = fluxRecord2.getValueByKey("_value").toString()
                        println("  Column $column")
                    }
                }
            }
        }
    }


    @Test
    fun testQuery() {
        val flux = "from(bucket:\"sample\") |> range(start: 0)"

        val queryApi = influxDBClient!!.queryApi

        val tables = queryApi.query(flux)
        for (fluxTable in tables) {
            println("### Table $fluxTable")
            val records = fluxTable.records
            for (fluxRecord in records) {
                println(fluxRecord.time.toString() + ": " + fluxRecord.values)
            }
        }

        influxDBClient!!.close()
    }
}
