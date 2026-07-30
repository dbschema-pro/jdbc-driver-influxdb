package com.wisecoders.jdbc.influxdb

import com.influxdb.client.InfluxDBClient
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify

class InfluxConnectionTest {

    @Test
    fun closeClosesClientAndSetsClosed() {
        val client = mock(InfluxDBClient::class.java)
        val connection = InfluxConnection(client, -30)

        assertThat(connection.isClosed).isFalse()

        connection.close()

        assertThat(connection.isClosed).isTrue()
        verify(client).close()
    }

    @Test
    fun doubleCloseIsIdempotent() {
        val client = mock(InfluxDBClient::class.java)
        val connection = InfluxConnection(client, -30)

        connection.close()
        connection.close()

        verify(client).close()
    }
}
