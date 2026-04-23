# Project Mindmap: Trading Streams

A timed graph of the thinking behind this project — from first question to full test suite.

```mermaid
flowchart TD
    START([I studied Kafka but Satispay uses SNS+SQS\nand it works fine for most things]) --> Q1

    subgraph PHASE1 [Phase 1 — Building the mental model]
        Q1[When is Kafka actually useful?] --> Q2[Real examples: CDC, fraud,\nactivity tracking, analytics]
        Q2 --> Q3[How does LinkedIn activity tracking work?]
        Q3 --> Q4[Satispay uses Redshift pipeline —\nis that the same thing?]
        Q4 --> Q5[What is the advantage of\nreal-time streaming ML vs batch?]
        Q5 --> INSIGHT1([Key insight: Kafka wins when\nthe log itself has value —\nnot just delivery])
    end

    INSIGHT1 --> DECISION1

    subgraph PHASE2 [Phase 2 — Project ideation]
        DECISION1{What project\nshould I build?} --> FRAUD[Fraud detection pipeline]
        DECISION1 --> STOCKS[Real-time stock prices]

        FRAUD --> FRAUD1[Velocity checks need Redis,\nnot Kafka]
        FRAUD1 --> FRAUD2[ML model is stateless —\nfeatures computed separately]
        FRAUD2 --> FRAUD3([Pivot: Kafka is a supporting\nactor here, not the star])

        STOCKS --> STOCKS1[Real-time WebSocket feed\nfor connected clients]
        STOCKS --> STOCKS2[Top-5 most active stocks\nrolling 5-minute window]
        STOCKS1 & STOCKS2 --> INSIGHT2([Key insight: one Kafka topic,\ntwo independent consumer groups\n= perfect showcase of Kafka's power])
    end

    INSIGHT2 --> PHASE3

    subgraph PHASE3 [Phase 3 — Architecture and building]
        ARCH[Define components:\nexchange-simulator\nwebsocket-service\nstream-processor\nanalytics-api] --> SKETCH[System design sketch:\nKafka → WebSocket + Redis pub/sub → clients\nKafka → Kafka Streams → Redis sorted set → API]
        SKETCH --> SCAFFOLD[Full Kotlin + Spring Boot scaffold]
        SCAFFOLD --> SCALE[Realistic scale:\n9000 symbols, 3 tiers,\n16 producer threads]
        SCALE --> NSPRING[Curiosity: what does\nSpring actually hide?\n→ no-Spring branch experiment]
        NSPRING --> NSPRING1[ShutdownHook vs @PreDestroy\nKafkaProducer vs KafkaTemplate\nProperties vs application.yml]
    end

    PHASE3 --> PHASE4

    subgraph PHASE4 [Phase 4 — Going deeper into internals]
        INT1[How does KafkaTemplate\nknow host and port?] --> INT2[Partitions: one per key\nor configurable?]
        INT2 --> INT3[KRaft vs Zookeeper:\nwhy are we using the old way?]
        INT3 --> INT4[Confluent vs Apache image]
        INT4 --> INT5[Producer: fire-and-forget?\nsync? async? idempotent?]
        INT5 --> INT6[Kafka buffer mechanics:\nRecordAccumulator, linger.ms,\nmax.block.ms, buffer.memory]
        INT6 --> INSIGHT3([Key insight: send is async by default\nbut blocks when buffer is full —\nthis is why shutdownNow does not work])
    end

    PHASE4 --> PHASE5

    subgraph PHASE5 [Phase 5 — Testing pyramid]
        TEST1[TopologyTestDriver:\npure unit test, no containers\nsentinel record to advance stream time\nmock Redis with MockK] --> TEST2[Component tests:\nTestcontainers per service\nreal Kafka and Redis\n@DynamicPropertySource]
        TEST2 --> TEST3[Full pipeline integration test:\nproduce ticks → Kafka Streams →\nRedis leaderboard → assert ranking\nAwaitility for async assertions]
        TEST3 --> BUG1[Bug: errors keep appearing\nafter shutdown]
        BUG1 --> BUG2[Root cause: Spring Kafka catches\nexceptions internally, send never throws\n→ loop never exits]
        BUG2 --> FIX([@Volatile running flag:\nstop loop before destroying producer])
    end

    subgraph DECISIONS [Key decisions made]
        D1[Kotlin over Java]
        D2[Apache Kafka image over Confluent\nin docker-compose\nbut Confluent in Testcontainers]
        D3[KRaft over Zookeeper]
        D4[Redis sorted set for leaderboard\nnot Kafka Streams state store]
        D5[suppress untilWindowCloses\nfor correct 5-min window counts]
        D6[no-Spring branch to understand\nwhat abstractions hide]
    end

    subgraph PROFILE [What this project reveals about the engineer]
        P1[Drawn to systems problems:\nthroughput, correctness, scale]
        P2[Questions the why, not just the how]
        P3[Experiments to understand abstractions]
        P4[Builds a testing pyramid instinctively]
        P5[Connects theory to real companies:\nTrade Republic, LinkedIn, Uber]
    end
```

---

## Timeline of key turning points

| Moment | What happened |
|---|---|
| Start | "I studied Kafka but SNS+SQS seems fine" |
| First insight | Kafka wins when the log itself has value, not just delivery |
| Pivot | Fraud pipeline is more ML engineering than SWE — wrong project |
| Crystallization | Stock prices + top-5 = one topic, two consumer groups — perfect |
| Ambition | Simulate Trade Republic's 9000 symbols, not 8 fake ones |
| Curiosity | What does Spring actually hide? → no-Spring branch |
| Quality | Full testing pyramid without being asked |
| Deep debugging | Kafka buffer mechanics, @Volatile flag, shutdown race conditions |

## The core mental model built

```
SNS+SQS  →  just delivery, fire and forget, managed
Kafka    →  durable log, replayable, stateful processing, ordering guarantees

Redis pub/sub   →  real-time fan-out to millions of connections
Kafka Streams   →  stateful computation on a stream
RocksDB         →  local state store inside Kafka Streams, persisted to disk
TopologyTestDriver → test stream logic without any infrastructure
```
