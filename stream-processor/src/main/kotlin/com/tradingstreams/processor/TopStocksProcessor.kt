package com.tradingstreams.processor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.springframework.beans.factory.annotation.Value
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Suppressed
import org.apache.kafka.streams.kstream.TimeWindows
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class TopStocksProcessor(
    private val redisTemplate: StringRedisTemplate,
    private val meterRegistry: MeterRegistry,
    @Value("\${app.window.duration-seconds:300}") private val windowDurationSeconds: Long
) {

    @Bean
    fun buildPipeline(builder: StreamsBuilder): KStream<String, String> {
        val stream = builder.stream<String, String>("price-ticks")

        stream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(windowDurationSeconds)))
            .count(Materialized.with(Serdes.String(), Serdes.Long()))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .foreach { key, count ->
                val start = System.nanoTime()
                redisTemplate.opsForZSet().add("leaderboard", key.key(), count.toDouble())
                meterRegistry.timer("redis.leaderboard.write").record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
            }

        return stream
    }
}
