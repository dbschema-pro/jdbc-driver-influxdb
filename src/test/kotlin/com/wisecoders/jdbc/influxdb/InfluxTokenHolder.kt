package com.wisecoders.jdbc.influxdb

import java.io.File
import java.util.Properties

/**
 * Test token loaded from (first match):
 * - environment variable INFLUX_TOKEN
 * - system property influx.token
 * - gradle.properties key influxToken
 * - local file influx.local.properties (not committed; see influx.local.properties.example)
 */
object InfluxTokenHolder {

    val INFLUX_TOKEN: String? = sequenceOf(
        System.getenv("INFLUX_TOKEN"),
        System.getProperty("influx.token"),
        readGradleProperty("influxToken"),
        readLocalPropertiesFile(),
    ).firstOrNull { !it.isNullOrBlank() }

    private fun readGradleProperty(key: String): String? {
        val file = File("gradle.properties")
        if (!file.isFile) return null
        return Properties().apply { file.inputStream().use { load(it) } }.getProperty(key)
    }

    private fun readLocalPropertiesFile(): String? {
        val file = File("influx.local.properties")
        if (!file.isFile) return null
        return Properties().apply { file.inputStream().use { load(it) } }.getProperty("influxToken")
    }
}
