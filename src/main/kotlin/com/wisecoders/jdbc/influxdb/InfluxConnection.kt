package com.wisecoders.jdbc.influxdb

import com.influxdb.client.InfluxDBClient
import com.wisecoders.common_jdbc.jvm.sql.AbstractConnection
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class InfluxConnection(
    val client: InfluxDBClient,
    val startDays: Int
) : AbstractConnection() {

    @Throws(SQLException::class)
    override fun createStatement(): Statement {
        return InfluxPreparedStatement(this, "")
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String): PreparedStatement {
        return InfluxPreparedStatement(this, sql)
    }

    @Throws(SQLException::class)
    override fun getAutoCommit(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun commit() {
    }

    @Throws(SQLException::class)
    override fun rollback() {
    }

    @Throws(SQLException::class)
    override fun close() {
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getMetaData(): DatabaseMetaData {
        return InfluxMetaData(this)
    }

    @Throws(SQLException::class)
    override fun setReadOnly(readOnly: Boolean) {
    }

    @Throws(SQLException::class)
    override fun isReadOnly(): Boolean {
        return false
    }



}
