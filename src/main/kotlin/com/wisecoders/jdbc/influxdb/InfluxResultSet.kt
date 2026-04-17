package com.wisecoders.jdbc.influxdb

import com.influxdb.query.FluxRecord
import com.influxdb.query.FluxTable
import com.wisecoders.common_jdbc.jvm.sql.AbstractResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class InfluxResultSet(private val fluxTables: List<FluxTable>) : AbstractResultSet() {
    private var posTable = -1
    private var posRecord = -1

    private var fluxTable: FluxTable? = null
    private var fluxRecord: FluxRecord? = null
    private val resultSetMetaData = InfluxResultSetMetaData(this)

    @Throws(SQLException::class)
    override fun next(): Boolean {
        var doLoop: Boolean
        do {
            if (fluxTable == null) {
                posTable++
                if (posTable < fluxTables.size) {
                    fluxTable = fluxTables[posTable]
                    posRecord = -1
                } else {
                    return false
                }
            }
            posRecord++
            if (posRecord >= fluxTable!!.records.size) {
                fluxTable = null
                doLoop = true
            } else {
                doLoop = false
            }
        } while (doLoop)
        fluxRecord = fluxTable!!.records[posRecord]
        return true
    }

    val oneFluxRecord: FluxRecord?
        get() {
            if (fluxRecord != null) return fluxRecord
            var fluxTable = this.fluxTable
            if (fluxTable == null) {
                fluxTable = fluxTables[0]
            }
            return fluxTable.records[0]
        }

    override fun getMetaData(): ResultSetMetaData {
        return resultSetMetaData
    }


    override fun close() {
    }

    @Throws(SQLException::class)
    override fun wasNull(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getString(columnIndex: Int): String? {
        return fluxRecord!!.values[resultSetMetaData.getColumnName(columnIndex)]?.toString()
    }

    @Throws(SQLException::class)
    override fun getString(columnLabel: String?): String? {
        return fluxRecord!!.values[columnLabel]?.toString()
    }

    @Throws(SQLException::class)
    override fun getDouble(columnLabel: String?): Double {
        val value: Any? = fluxRecord!!.values[columnLabel]
        return if (value is Number) value.toDouble() else -1.0
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnIndex: Int): Boolean {
        val value = getObject(columnIndex)
        return if (value is Boolean) value else false
    }

    @Throws(SQLException::class)
    override fun getByte(columnIndex: Int): Byte {
        return 0
    }

    @Throws(SQLException::class)
    override fun getShort(columnIndex: Int): Short {
        val value = getObject(columnIndex)
        return if (value is Number) value.toShort() else -1
    }

    @Throws(SQLException::class)
    override fun getInt(columnIndex: Int): Int {
        val value = getObject(columnIndex)
        return if (value is Number) value.toInt() else -1
    }

    @Throws(SQLException::class)
    override fun getLong(columnIndex: Int): Long {
        val value = getObject(columnIndex)
        return if (value is Number) value.toLong() else -1
    }

    @Throws(SQLException::class)
    override fun getFloat(columnIndex: Int): Float {
        return 0f
    }

    @Throws(SQLException::class)
    override fun getDouble(columnIndex: Int): Double {
        val value = getObject(columnIndex)
        return if (value is Number) value.toDouble() else -1.0
    }

}
