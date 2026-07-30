package com.wisecoders.jdbc.influxdb

import com.influxdb.client.InfluxDBClient
import com.influxdb.exceptions.InfluxException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`

class InfluxMetaDataTest {

    @Nested
    inner class DatabaseVersion {

        @Test
        fun reportsTheVersionOfTheConnectedServer() {
            val metaData: InfluxMetaData = metaDataReportingServerVersion("2.7.11")

            assertThat(metaData.getDatabaseProductVersion()).isEqualTo("2.7.11")
        }

        @Test
        fun reportsUnknownWhenTheServerVersionCannotBeRead() {
            val metaData: InfluxMetaData = metaDataFailingToReportServerVersion()

            assertThat(metaData.getDatabaseProductVersion()).isEqualTo("unknown")
            assertThat(metaData.getDatabaseMajorVersion()).isZero()
            assertThat(metaData.getDatabaseMinorVersion()).isZero()
        }

        @Test
        fun readsTheServerVersionOnlyOnce() {
            val client: InfluxDBClient = clientReportingVersion("2.7.11")
            val metaData = InfluxMetaData(InfluxConnection(client, START_DAYS))

            metaData.getDatabaseProductVersion()
            metaData.getDatabaseMajorVersion()
            metaData.getDatabaseMinorVersion()

            verify(client, times(1)).version()
        }

        @ParameterizedTest
        @CsvSource(
            "2.7.11, 2, 7",
            "3.0, 3, 0",
            "1, 1, 0",
            "2.7.11-alpha.1, 2, 7",
        )
        fun splitsTheServerVersionIntoMajorAndMinor(
            serverVersion: String,
            expectedMajor: Int,
            expectedMinor: Int,
        ) {
            val metaData: InfluxMetaData = metaDataReportingServerVersion(serverVersion)

            assertThat(metaData.getDatabaseMajorVersion()).isEqualTo(expectedMajor)
            assertThat(metaData.getDatabaseMinorVersion()).isEqualTo(expectedMinor)
        }
    }

    @Nested
    inner class Identification {

        @Test
        fun namesTheDatabaseAndTheDriver() {
            val metaData: InfluxMetaData = metaDataReportingServerVersion("2.7.11")

            assertThat(metaData.getDatabaseProductName()).isEqualTo("InfluxDB")
            assertThat(metaData.getDriverName()).isEqualTo("InfluxDB JDBC Driver")
            assertThat(metaData.getDriverVersion()).isEqualTo("1.0")
            assertThat(metaData.getDriverMajorVersion()).isEqualTo(1)
            assertThat(metaData.getDriverMinorVersion()).isZero()
        }
    }

    private fun metaDataReportingServerVersion(serverVersion: String): InfluxMetaData {
        return InfluxMetaData(InfluxConnection(clientReportingVersion(serverVersion), START_DAYS))
    }

    private fun metaDataFailingToReportServerVersion(): InfluxMetaData {
        val client: InfluxDBClient = mock(InfluxDBClient::class.java)
        `when`(client.version()).thenThrow(InfluxException("cannot reach the server"))
        return InfluxMetaData(InfluxConnection(client, START_DAYS))
    }

    private fun clientReportingVersion(serverVersion: String): InfluxDBClient {
        val client: InfluxDBClient = mock(InfluxDBClient::class.java)
        `when`(client.version()).thenReturn(serverVersion)
        return client
    }

    private companion object {
        const val START_DAYS = -30
    }
}
