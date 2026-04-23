package com.tradingstreams.processor

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ZSetOperations
import java.time.Instant
import java.util.Properties

class TopStocksProcessorTopologyTest {

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<String, String>

    private val zSetOps = mockk<ZSetOperations<String, String>>(relaxed = true)
    private val redisTemplate = mockk<StringRedisTemplate>()
    private val timer = mockk<Timer>(relaxed = true)
    private val meterRegistry = mockk<MeterRegistry>()

    private val windowSeconds = 10L

    @BeforeEach
    fun setup() {
        every { redisTemplate.opsForZSet() } returns zSetOps
        every { meterRegistry.timer(any<String>()) } returns timer

        val builder = StreamsBuilder()
        TopStocksProcessor(redisTemplate, meterRegistry, windowSeconds).buildPipeline(builder)

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "test-processor")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        }

        testDriver = TopologyTestDriver(builder.build(), props)
        inputTopic = testDriver.createInputTopic("price-ticks", StringSerializer(), StringSerializer())
    }

    @AfterEach
    fun teardown() {
        testDriver.close()
    }

    @Test
    fun `counts ticks per symbol and writes to redis when window closes`() {
        val windowMs = windowSeconds * 1000

        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(0))
        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(1000))
        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(2000))
        inputTopic.pipeInput("TSLA", "{}", Instant.ofEpochMilli(3000))
        inputTopic.pipeInput("TSLA", "{}", Instant.ofEpochMilli(4000))

        // advance stream time past window end to trigger suppression
        inputTopic.pipeInput("_sentinel", "{}", Instant.ofEpochMilli(windowMs + 1))

        verify { zSetOps.add("leaderboard", "AAPL", 3.0) }
        verify { zSetOps.add("leaderboard", "TSLA", 2.0) }
    }

    @Test
    fun `does not emit results before window closes`() {
        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(0))
        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(1000))

        verify(exactly = 0) { zSetOps.add(any(), any(), any()) }
    }

    @Test
    fun `counts each symbol independently`() {
        val windowMs = windowSeconds * 1000

        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(0))
        inputTopic.pipeInput("GOOGL", "{}", Instant.ofEpochMilli(1000))
        inputTopic.pipeInput("AAPL", "{}", Instant.ofEpochMilli(2000))
        inputTopic.pipeInput("GOOGL", "{}", Instant.ofEpochMilli(3000))
        inputTopic.pipeInput("GOOGL", "{}", Instant.ofEpochMilli(4000))

        inputTopic.pipeInput("_sentinel", "{}", Instant.ofEpochMilli(windowMs + 1))

        verify { zSetOps.add("leaderboard", "AAPL", 2.0) }
        verify { zSetOps.add("leaderboard", "GOOGL", 3.0) }
    }
}
