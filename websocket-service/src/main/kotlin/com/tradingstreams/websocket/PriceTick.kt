package com.tradingstreams.websocket

data class PriceTick(
    val symbol: String,
    val price: Double,
    val timestamp: Long
)
