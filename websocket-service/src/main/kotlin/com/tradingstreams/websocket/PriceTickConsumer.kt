package com.tradingstreams.websocket

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component

@Component
class PriceTickConsumer(private val messagingTemplate: SimpMessagingTemplate) {

    @KafkaListener(topics = ["price-ticks"], groupId = "websocket-service")
    fun consume(tick: PriceTick) {
        messagingTemplate.convertAndSend("/topic/prices/${tick.symbol}", tick)
    }
}
