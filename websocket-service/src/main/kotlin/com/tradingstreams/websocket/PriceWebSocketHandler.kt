package com.tradingstreams.websocket

import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

@Component
class PriceWebSocketHandler : TextWebSocketHandler() {

    private val sessions = ConcurrentHashMap<String, CopyOnWriteArrayList<WebSocketSession>>()

    override fun afterConnectionEstablished(session: WebSocketSession) {
        val symbol = session.uri?.path?.substringAfterLast("/")?.uppercase() ?: return
        sessions.getOrPut(symbol) { CopyOnWriteArrayList() }.add(session)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        val symbol = session.uri?.path?.substringAfterLast("/")?.uppercase() ?: return
        sessions[symbol]?.remove(session)
    }

    fun broadcast(symbol: String, message: String) {
        sessions[symbol]?.forEach { session ->
            runCatching {
                if (session.isOpen) session.sendMessage(TextMessage(message))
            }
        }
    }
}
