package com.tradingstreams.simulator

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import java.util.UUID

@SpringBootTest
@Testcontainers
class PriceTickProducerTest {

    companion object {
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))

        @DynamicPropertySource
        @JvmStatic
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
        }
    }

    @Test
    fun `produces price ticks to kafka topic`() {
        Thread.sleep(2000)

        val consumer = KafkaConsumer<String, String>(Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        })

        consumer.subscribe(listOf("price-ticks"))
        val records = consumer.poll(Duration.ofSeconds(5))
        consumer.close()

        assertThat(records.count()).isGreaterThan(0)

        val record = records.first()
        assertThat(SymbolRegistry.all).contains(record.key())

        val tick = jacksonObjectMapper().readValue(record.value(), PriceTick::class.java)
        assertThat(tick.symbol).isEqualTo(record.key())
        assertThat(tick.price).isGreaterThan(0.0)
        assertThat(tick.timestamp).isGreaterThan(0L)
    }

    @Test
    fun `produces ticks for multiple symbols`() {
        Thread.sleep(3000)

        val consumer = KafkaConsumer<String, String>(Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        })

        consumer.subscribe(listOf("price-ticks"))
        val records = consumer.poll(Duration.ofSeconds(5))
        consumer.close()

        val symbols = records.map { it.key() }.toSet()
        assertThat(symbols.size).isGreaterThan(1)
    }
}
