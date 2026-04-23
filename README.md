# Trading Streams

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

## System Architecture

```
Exchange Simulator  ──────────────────────────────────────────────────────────────────────
(9000 symbols,           Kafka Topic: price-ticks
 16 threads,         ┌──────────────────────────────────────────┐
 tiered frequency)   │  key = symbol, value = PriceTick JSON    │
        │            │  partitioned by symbol                   │
        └──────────► │  lz4 compression, idempotent producer    │
                     └──────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
          WebSocket Service                       Stream Processor
          (@KafkaListener)                        (Kafka Streams)
                    │                                       │
          fan-out to clients                  5-min tumbling window
          via STOMP + raw WS                  count ticks per symbol
                    │                         suppress until window closes
                    ▼                                       │
           Connected clients                               ▼
           (browser / load test)                     Redis Sorted Set
                                                      (leaderboard)
                                                           │
                                                           ▼
                                                     Analytics API
                                                  GET /api/top-stocks
                                                  returns top 5 by score
```

### Key design decisions

**Why Kafka and not SNS+SQS**
The price-ticks topic is a durable log. Both the WebSocket service and the stream processor consume it independently at their own pace. A new consumer (fraud detection, ML feature pipeline, audit log) can be added by connecting to the same topic with zero changes to the producer. With SNS+SQS each new consumer requires a new subscription and queue to be provisioned, and historical replay is impossible.

**Why Kafka Streams for the leaderboard**
The leaderboard requires a 5-minute tumbling window with correct aggregation. Kafka Streams runs the computation inside the stream-processor process using `suppress(untilWindowCloses)`, which guarantees results are emitted exactly once per window, not as running approximations. An alternative with Redis `INCR` and a cron job would give approximate results and lose counts on restart.

**Why Redis for the leaderboard store**
Kafka Streams writes the final window count into a Redis sorted set. Redis sorted sets support `ZREVRANGEBYSCORE` in O(log N), so the analytics API reads the top 5 in constant time under any load. The stream processor owns writes; the analytics API is read-only with no coordination needed.

**Why Redis pub/sub for WebSocket fan-out**
In a multi-instance deployment, a price tick consumed by instance A must reach clients connected to instance B. Redis pub/sub acts as the shared message bus between WebSocket service instances. Each instance subscribes to the channels for its connected symbols and forwards to local sessions.

## Testing Strategy

The project uses a three-layer testing pyramid:

**Unit: TopologyTestDriver** (`stream-processor`)
Tests the Kafka Streams topology in-process with no brokers. Uses `TopologyTestDriver` to pipe records with explicit timestamps and advance stream time to trigger window suppression. MockK stubs `StringRedisTemplate` to verify the correct counts reach Redis.

**Integration: Testcontainers** (`analytics-api`, `exchange-simulator`)
Spins up real Kafka (`confluentinc/cp-kafka:7.6.0`) and Redis (`redis:7.2`) containers per test class. Verifies the producer actually writes to Kafka and that the analytics API reads the correct leaderboard from Redis.

**End-to-end: Full pipeline** (`integration-tests`)
Starts all infrastructure containers, produces ticks with explicit timestamps via a raw `KafkaProducer`, advances stream time with a sentinel record, and uses `Awaitility` to assert the leaderboard appears in Redis within 30 seconds.

## Observability

Each service exposes Prometheus metrics at `/actuator/prometheus`. Prometheus scrapes all four services every 15 seconds. Key metrics:

- `kafka_producer_record_send_total` — total ticks produced
- `rate(kafka_producer_record_send_total[1m])` — producer throughput
- `kafka_producer_buffer_available_bytes` — buffer headroom
- `kafka_producer_record_queue_time_avg` — time records wait before batching
- `redis_leaderboard_write_seconds` — stream-processor write latency
- `jvm_memory_used_bytes` — heap pressure per service
- `kafka_consumer_group_lag` — consumer lag (most important SRE signal)

## Load Testing

k6 scripts in `load-tests/` cover two scenarios:

**analytics-api.js:** baseline 50 VUs ramping to 500 VUs spike. Thresholds: p95 latency under 50ms, error rate under 1%.

**websocket.js:** WebSocket clients ramping from 0 to 1000 VUs subscribing to individual symbol streams. Threshold: p95 message latency under 500ms.