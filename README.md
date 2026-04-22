# TradeStream

## Problem Statement

FinEdge is a retail trading platform with millions of active users worldwide. As the platform grows, two core product requirements have emerged:

**1. Live price updates**
Users expect to see stock prices update in real-time on their app. The current implementation polls a REST API every 5 seconds: it is slow, expensive, and does not scale. With millions of connected clients and thousands of price ticks per second coming from exchanges, FinEdge needs to deliver price updates to every connected client with sub-second latency.

**2. Top 5 most active stocks**
The home screen displays the top 5 most active stocks, ranked by tick volume in the last 5 minutes. This gives users a live pulse of what the market is doing. The current implementation runs a database query on every page load, which becomes a bottleneck under load. FinEdge needs this to be computed continuously and served instantly.

## Functional Requirements

- Ingest high-frequency price ticks from a simulated exchange feed
- Deliver live price updates to connected clients with sub-second latency
- Compute the top 5 most active stocks by tick volume in a rolling 5-minute window
- The system must remain correct and responsive under high load
- No data loss: a tick that enters the system must be processed

## Non-Functional Requirements

- **Throughput:** ingest 10,000+ price ticks per second from the simulated exchange
- **Latency:** price updates must reach connected clients within 500ms of being produced
- **Concurrency:** support 1,000,000+ simultaneous connected clients
- **Availability:** no single point of failure. The system must continue processing if one component or service restarts
- **Durability:** no tick is lost in transit. If a downstream service is temporarily unavailable, events must be buffered and replayed
- **Scalability:** the system must scale horizontally, meaning adding more resources should increase throughput linearly
- **Consistency:** the top 5 leaderboard must reflect a correct rolling 5-minute window, not an approximation

## Non-requirements

- Authentication and authorization
- Persistent user accounts
- Order execution or trade settlement
- Historical price storage and charting