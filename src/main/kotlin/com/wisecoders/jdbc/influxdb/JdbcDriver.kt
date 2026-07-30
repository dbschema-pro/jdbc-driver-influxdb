package com.wisecoders.jdbc.influxdb

import com.influxdb.client.InfluxDBClientFactory
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.Properties
import java.util.logging.Logger

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class JdbcDriver : Driver {
    /**
     * Connect to the database using a URL like :
     * https://{HOST}:{PORT}?org={DB}&token={PARAM}&days={PARAM2}
     */
    @Throws(SQLException::class)
    override fun connect(
        url: String?,
        info: Properties?,
    ): Connection? {
        if (url == null || !acceptsURL(url)) return null

        // Ensure info is not null
        val parameters = info ?: Properties()

        // Parse URL query parameters
        url.substringAfter("?", "").split("&").forEach { pair ->
            val (key, value) = pair.split("=", limit = 2).takeIf { it.size == 2 } ?: return@forEach
            if (!parameters.containsKey(key)) {
                parameters[key] = URLDecoder.decode(value, StandardCharsets.UTF_8)
                LOGGER.atDebug().setMessage("Param $key=${maskParamForLog(key, parameters[key] as String?)}").log()
            }
        }

        val userName = parameters["user"] as String?
        val password = parameters["password"] as String?
        val token = parameters["token"] as String?
        val org = parameters["org"] as String?
        val startDaysStr = parameters[DAYS] as String?

        LOGGER.atInfo().setMessage(
            "Connection URL=${maskUrlForLog(url)} user=$userName org=$org days=$startDaysStr",
        ).log()

        val startDays = startDaysStr?.toIntOrNull()?.let { if (it > 0) -it else it } ?: -30
        LOGGER.atInfo().setMessage(("Use days=$startDays")).log()

        // Create InfluxDB client
        val client = when {
            userName != null && password != null -> InfluxDBClientFactory.create(url, userName, password.toCharArray())
            token != null -> InfluxDBClientFactory.create(url, token.toCharArray(), org)
            else -> InfluxDBClientFactory.create(url)
        }

        return InfluxConnection(client, startDays)
    }


    /**
     * URLs accepted are HTTP(S) InfluxDB endpoints, e.g. http://host:8086?org=myorg&token=...
     *
     * @see java.sql.Driver.acceptsURL
     */
    @Throws(SQLException::class)
    override fun acceptsURL(url: String): Boolean {
        return url.startsWith("http")
    }

    /**
     * @see java.sql.Driver.getPropertyInfo
     */
    @Throws(SQLException::class)
    override fun getPropertyInfo(
        url: String,
        info: Properties,
    ): Array<DriverPropertyInfo>? {
        return null
    }

    /**
     * @see java.sql.Driver.getMajorVersion
     */
    override fun getMajorVersion(): Int {
        return 1
    }

    /**
     * @see java.sql.Driver.getMinorVersion
     */
    override fun getMinorVersion(): Int {
        return 0
    }

    /**
     * @see java.sql.Driver.jdbcCompliant
     */
    override fun jdbcCompliant(): Boolean {
        return true
    }

    @Throws(SQLFeatureNotSupportedException::class)
    override fun getParentLogger(): Logger? {
        return null
    }

    companion object {
        private val LOGGER: org.slf4j.Logger = slf4jLogger()
        private val SENSITIVE_PARAM = Regex("(?i)(password|token|secretaccesskey)=([^&]*)")

        init {
            DriverManager.registerDriver(JdbcDriver())
        }

        const val DAYS: String = "days"

        private fun maskParamForLog(key: String, value: String?): String {
            if (value == null) return "null"
            return when (key.lowercase()) {
                "password", "token", "secretaccesskey" -> "*".repeat(value.length)
                else -> value
            }
        }

        private fun maskUrlForLog(url: String): String =
            SENSITIVE_PARAM.replace(url) { match ->
                match.groupValues[1] + "=" + "*".repeat(match.groupValues[2].length)
            }
    }
}
