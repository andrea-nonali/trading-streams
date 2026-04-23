package com.tradingstreams.simulator

object SymbolRegistry {

    val highFrequency: List<String> = listOf(
        "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "NFLX",
        "AMD", "INTC", "ORCL", "ADBE", "CRM", "PYPL", "SHOP", "SPOT",
        "UBER", "LYFT", "SNAP", "TWTR", "COIN", "HOOD", "SOFI", "PLTR",
        "SQ", "ROKU", "ZM", "DOCU", "CRWD", "DDOG", "NET", "SNOW",
        "MDB", "OKTA", "GTLB", "U", "RBLX", "ABNB", "DASH", "RIVN",
        "LCID", "NIO", "XPEV", "LI", "BABA", "JD", "PDD", "BIDU",
        "JPM", "BAC", "GS", "MS", "C", "WFC", "BRK", "V",
        "MA", "AXP", "BLK", "SCHW", "IBKR", "CME", "ICE", "NDAQ",
        "XOM", "CVX", "BP", "SHEL", "TTE", "ENB", "SLB", "HAL",
        "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "GILD", "AMGN"
    )

    val mediumFrequency: List<String> = ('A'..'Z').flatMap { a ->
        ('A'..'Z').map { b -> "$a${b}X" }
    }.take(1000)

    val lowFrequency: List<String> = ('A'..'Z').flatMap { a ->
        ('A'..'Z').flatMap { b ->
            ('A'..'Z').map { c -> "$a$b$c" }
        }
    }.filter { it !in highFrequency && !mediumFrequency.contains(it) }.take(7930)

    val all: List<String> = highFrequency + mediumFrequency + lowFrequency
}
