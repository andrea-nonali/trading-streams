package com.tradingstreams

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Suppressed
import org.apache.kafka.streams.kstream.TimeWindows
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

@Testcontainers
class FullPipelineTest {

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))

        @Container
        @JvmStatic
        val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7.2"))
            .withExposedPorts(6379)
    }

    private lateinit var streams: KafkaStreams
    private lateinit var redisTemplate: StringRedisTemplate
    private lateinit var connectionFactory: LettuceConnectionFactory

    private val windowSeconds = 10L

    @BeforeEach
    fun setup() {
        connectionFactory = LettuceConnectionFactory(redis.host, redis.getMappedPort(6379))
        connectionFactory.afterPropertiesSet()
        redisTemplate = StringRedisTemplate(connectionFactory).also { it.afterPropertiesSet() }
        streams = buildStreams()
        streams.start()
    }

    @AfterEach
    fun teardown() {
        streams.close()
        redisTemplate.delete("leaderboard")
        connectionFactory.destroy()
    }

    @Test
    fun `ticks flow through stream processor to redis leaderboard with correct ranking`() {
        val producer = createProducer()
        val now = System.currentTimeMillis()

        repeat(10) { producer.send(ProducerRecord("price-ticks", null, now + it * 100L, "AAPL", "{}")) }
        repeat(7)  { producer.send(ProducerRecord("price-ticks", null, now + it * 100L, "TSLA", "{}")) }
        repeat(3)  { producer.send(ProducerRecord("price-ticks", null, now + it * 100L, "GOOGL", "{}")) }

        // advance stream time past window end to trigger suppression
        producer.send(ProducerRecord("price-ticks", null, now + Duration.ofSeconds(windowSeconds + 1).toMillis(), "_sentinel", "{}"))
        producer.flush()
        producer.close()

        await().atMost(30, TimeUnit.SECONDS).until {
            (redisTemplate.opsForZSet().size("leaderboard") ?: 0) >= 3
        }

        val topStocks = redisTemplate.opsForZSet()
            .reverseRangeWithScores("leaderboard", 0, 2)
            ?.map { it.value!! }

        assertThat(topStocks).containsExactly("AAPL", "TSLA", "GOOGL")
    }

    @Test
    fun `tick counts match exactly what was produced`() {
        val producer = createProducer()
        val now = System.currentTimeMillis()

        repeat(5) { producer.send(ProducerRecord("price-ticks", null, now + it * 100L, "NVDA", "{}")) }

        producer.send(ProducerRecord("price-ticks", null, now + Duration.ofSeconds(windowSeconds + 1).toMillis(), "_sentinel", "{}"))
        producer.flush()
        producer.close()

        await().atMost(30, TimeUnit.SECONDS).until {
            redisTemplate.opsForZSet().score("leaderboard", "NVDA") != null
        }

        val score = redisTemplate.opsForZSet().score("leaderboard", "NVDA")
        assertThat(score).isEqualTo(5.0)
    }

    private fun buildStreams(): KafkaStreams {
        val builder = StreamsBuilder()

        builder.stream<String, String>("price-ticks")
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(windowSeconds)))
            .count(Materialized.with(Serdes.String(), Serdes.Long()))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .foreach { key, count ->
                redisTemplate.opsForZSet().add("leaderboard", key.key(), count.toDouble())
            }

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-test-${UUID.randomUUID()}")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams-test").toString())
        }

        return KafkaStreams(builder.build(), props)
    }

    private fun createProducer(): KafkaProducer<String, String> {
        return KafkaProducer(Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        })
    }
}
