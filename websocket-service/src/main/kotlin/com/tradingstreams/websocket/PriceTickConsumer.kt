package com.tradingstreams.websocket

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component

@Component
class PriceTickConsumer(
    private val messagingTemplate: SimpMessagingTemplate,
    private val webSocketHandler: PriceWebSocketHandler,
    private val objectMapper: ObjectMapper
) {

    @KafkaListener(topics = ["price-ticks"], groupId = "websocket-service")
    fun consume(tick: PriceTick) {
        messagingTemplate.convertAndSend("/topic/prices/${tick.symbol}", tick)
        webSocketHandler.broadcast(tick.symbol, objectMapper.writeValueAsString(tick))
    }
}
