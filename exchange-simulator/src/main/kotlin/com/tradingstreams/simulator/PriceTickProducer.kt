package com.tradingstreams.simulator

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import kotlin.random.Random

@Component
class PriceTickProducer(private val kafkaTemplate: KafkaTemplate<String, PriceTick>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val counter = LongAdder()
    private val executor = Executors.newFixedThreadPool(16)
    private val metricsExecutor = Executors.newSingleThreadScheduledExecutor()

    @PostConstruct
    fun start() {
        scheduleProducers(SymbolRegistry.highFrequency, delayMs = 1)
        scheduleProducers(SymbolRegistry.mediumFrequency, delayMs = 10)
        scheduleProducers(SymbolRegistry.lowFrequency, delayMs = 100)

        metricsExecutor.scheduleAtFixedRate({
            log.info("throughput: {} msgs/sec | total symbols: {}", counter.sumThenReset(), SymbolRegistry.all.size)
        }, 1, 1, TimeUnit.SECONDS)
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
                counter.increment()
            }
            if (delayMs > 0) Thread.sleep(delayMs)
        }
    }

    @PreDestroy
    fun stop() {
        executor.shutdownNow()
        metricsExecutor.shutdownNow()
    }

    companion object {
        const val TOPIC = "price-ticks"
    }
}
