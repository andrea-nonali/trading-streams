package com.tradingstreams.simulator

data class PriceTick(
    val symbol: String,
    val price: Double,
    val timestamp: Long = System.currentTimeMillis()
)
