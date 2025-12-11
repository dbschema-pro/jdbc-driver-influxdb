package com.wisecoders.jdbc.influxdb

import com.wisecoders.common_jdbc.jvm.sql.AbstractResultSetMetaData
import java.sql.SQLException
import java.sql.Types

/**
 * Copyright  [Wise Coders GmbH](https://wisecoders.com)
 * Driver is used in the  [DbSchema Database Designer](https://dbschema.com)
 * Free to be used by everyone.
 * Code modifications allowed only to [GitHub repository](https://github.com/wise-coders/influxdb-jdbc-driver)
 */
class InfluxResultSetMetaData(private val influxResultSet: InfluxResultSet) :
    AbstractResultSetMetaData() {

    private val columnNames: MutableList<String> = ArrayList()
    private val columnClasses: MutableMap<String, Class<*>> = HashMap()

    private fun init() {
        val fluxRecord = influxResultSet.oneFluxRecord
        if (fluxRecord != null) {
            for (columnName in fluxRecord.values.keys) {
                if (!columnNames.contains(columnName)) {
                    columnNames.add(columnName)
                }
                if (!columnClasses.containsKey(columnName)) {
                    val value = fluxRecord.values[columnName]
                    if (value != null) columnClasses[columnName] = value.javaClass
                }
            }
        }
    }


    @Throws(SQLException::class)
    override fun getColumnCount(): Int {
        init()
        return columnNames.size
    }

    @Throws(SQLException::class)
    override fun getColumnName(column: Int): String {
        init()
        if (column < columnNames.size) {
            return columnNames[column]
        }
        return ""
    }

    @Throws(SQLException::class)
    override fun getColumnType(column: Int): Int {
        if (column < columnNames.size) {
            val cls = columnClasses[columnNames[column]]!!
            if (String::class.java == cls) return Types.VARCHAR
            else if (Double::class.java == cls) return Types.DOUBLE
            else if (Int::class.java == cls) return Types.DOUBLE
        }
        return Types.VARCHAR
    }

    @Throws(SQLException::class)
    override fun getColumnTypeName(column: Int): String {
        if (column < columnClasses.size) {
            val cls = columnClasses[columnNames[column]]
            return if (cls != null) cls.simpleName else "string"
        }
        return "string"
    }

}
