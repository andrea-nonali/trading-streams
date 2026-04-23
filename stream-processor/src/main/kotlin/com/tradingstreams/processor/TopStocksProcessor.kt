package com.tradingstreams.processor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Suppressed
import org.apache.kafka.streams.kstream.TimeWindows
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

@Configuration
class TopStocksProcessor(private val redisTemplate: StringRedisTemplate) {

    @Bean
    fun buildPipeline(builder: StreamsBuilder): KStream<String, String> {
        val stream = builder.stream<String, String>("price-ticks")

        stream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.with(Serdes.String(), Serdes.Long()))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .foreach { key, count ->
                redisTemplate.opsForZSet().add("leaderboard", key.key(), count.toDouble())
            }

        return stream
    }
}
