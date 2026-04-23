package com.tradingstreams.analytics

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class AnalyticsApiApplication

fun main(args: Array<String>) {
    runApplication<AnalyticsApiApplication>(*args)
}
