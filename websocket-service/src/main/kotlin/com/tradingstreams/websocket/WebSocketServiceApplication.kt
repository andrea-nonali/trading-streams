package com.tradingstreams.websocket

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class WebSocketServiceApplication

fun main(args: Array<String>) {
    runApplication<WebSocketServiceApplication>(*args)
}
