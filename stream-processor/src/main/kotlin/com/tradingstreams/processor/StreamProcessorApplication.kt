package com.tradingstreams.processor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafkaStreams

@SpringBootApplication
@EnableKafkaStreams
class StreamProcessorApplication

fun main(args: Array<String>) {
    runApplication<StreamProcessorApplication>(*args)
}
