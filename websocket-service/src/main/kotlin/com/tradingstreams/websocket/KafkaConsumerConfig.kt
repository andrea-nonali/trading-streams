package com.tradingstreams.websocket

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class KafkaConsumerConfig(
    private val kafkaProperties: KafkaProperties,
    private val objectMapper: ObjectMapper,
) {

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, PriceTick> {
        val deserializer = JsonDeserializer(PriceTick::class.java, objectMapper).apply {
            addTrustedPackages("*")
            ignoreTypeHeaders()
        }
        val consumerFactory = DefaultKafkaConsumerFactory(
            kafkaProperties.buildConsumerProperties(null),
            StringDeserializer(),
            deserializer
        )
        return ConcurrentKafkaListenerContainerFactory<String, PriceTick>().also {
            it.consumerFactory = consumerFactory
        }
    }
}
