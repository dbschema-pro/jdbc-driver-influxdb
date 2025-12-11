package com.wisecoders.jdbc.influxdb

import com.influxdb.exceptions.NotFoundException
import com.wisecoders.common_jdbc.jvm.result_set.ArrayResultSet
import com.wisecoders.common_jdbc.jvm.sql.AbstractPreparedStatement
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.sql.ResultSet
import java.sql.SQLException
import org.slf4j.Logger

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class InfluxPreparedStatement(
    private val connection: InfluxConnection,
    private var query: String,
) : AbstractPreparedStatement() {

    @Throws(SQLException::class)
    override fun executeQuery(): ResultSet {
        rs = null
        if (query.trim().lowercase().matches("list\\s+organizations".toRegex())) {
            LOGGER.atInfo().setMessage("List organizations").log()

            val result = ArrayResultSet()
            result.setColumnNames(listOf("ORGANIZATIONS"))
            try {
                for (organization in connection.client.organizationsApi.findOrganizations()) {
                    result.addRow(listOf(organization.name.toString()))
                }
            } catch (_: NotFoundException) {
            }
            return result
        }

        val queryResult = connection.client.queryApi.query(query)
        return InfluxResultSet(queryResult)
    }

    @Throws(SQLException::class)
    override fun executeUpdate(): Int {
        return 0
    }

    @Throws(SQLException::class)
    override fun executeQuery(sql: String): ResultSet {
        this.query = sql
        rs = executeQuery()
        return rs!!
    }

    @Throws(SQLException::class)
    override fun execute(sql: String): Boolean {
        this.query = sql
        rs = executeQuery()
        return true
    }

    @Throws(SQLException::class)
    override fun getResultSet(): ResultSet {
        return rs!!
    }


    @Throws(SQLException::class)
    override fun executeUpdate(sql: String): Int {
        executeQuery()
        return 0
    }

    @Throws(SQLException::class)
    override fun execute(): Boolean {
        rs = executeQuery()
        return true
    }


    companion object {
        private val LOGGER: Logger = slf4jLogger()
    }
}
