plugins {
    alias(libs.plugins.wisecoders.commonGradle.jdbcDriver)
}

group = "com.wisecoders.jdbc-drivers"

jdbcDriver {
    dbId = "InfluxDB"
}

dependencies {
    implementation(libs.wisecoders.commonLib.commonSlf4j)
    implementation(libs.wisecoders.commonJdbc.commonJdbcJvm)
    implementation(libs.slf4j.api)
    implementation(libs.influxdb.clientJava)

     runtimeOnly(libs.logback.classic)
}
