package com.tradingstreams.simulator

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import kotlin.random.Random

@Component
class PriceTickProducer(private val kafkaTemplate: KafkaTemplate<String, PriceTick>) {

    private val executor = Executors.newFixedThreadPool(16)

    @PostConstruct
    fun start() {
        scheduleProducers(SymbolRegistry.highFrequency, delayMs = 1)
        scheduleProducers(SymbolRegistry.mediumFrequency, delayMs = 10)
        scheduleProducers(SymbolRegistry.lowFrequency, delayMs = 100)
    }

    private fun scheduleProducers(symbols: List<String>, delayMs: Long) {
        val prices = symbols.associateWith { Random.nextDouble(10.0, 1000.0) }.toMutableMap()
        val batches = symbols.chunked((symbols.size / 4).coerceAtLeast(1))
        batches.forEach { batch ->
            executor.submit { produceLoop(batch, prices, delayMs) }
        }
    }

    private fun produceLoop(symbols: List<String>, prices: MutableMap<String, Double>, delayMs: Long) {
        while (!Thread.currentThread().isInterrupted) {
            for (symbol in symbols) {
                val currentPrice = prices[symbol]!!
                val updatedPrice = currentPrice * (1 + Random.nextDouble(-0.002, 0.002))
                prices[symbol] = updatedPrice
                kafkaTemplate.send(TOPIC, symbol, PriceTick(symbol, updatedPrice))
            }
            if (delayMs > 0) Thread.sleep(delayMs)
        }
    }

    @PreDestroy
    fun stop() {
        executor.shutdownNow()
    }

    companion object {
        const val TOPIC = "price-ticks"
    }
}
