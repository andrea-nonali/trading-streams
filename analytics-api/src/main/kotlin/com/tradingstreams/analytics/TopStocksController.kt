package com.tradingstreams.analytics

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class StockRank(val symbol: String, val tickCount: Long)

@RestController
@RequestMapping("/api")
class TopStocksController(private val redisTemplate: StringRedisTemplate) {

    @GetMapping("/top-stocks")
    fun getTopStocks(): List<StockRank> {
        return redisTemplate.opsForZSet()
            .reverseRangeWithScores("leaderboard", 0, 4)
            ?.map { StockRank(it.value!!, it.score!!.toLong()) }
            ?: emptyList()
    }
}
