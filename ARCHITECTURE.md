# Architecture

## Full pipeline

```mermaid
flowchart TB
    subgraph ING["Ingestion"]
        DEV["Synthetic IoT Devices"]
        PROD["Python Producer<br/>EVENTS_PER_SECOND<br/>anomaly + duplicate injection"]
        RAW["Kafka Topic<br/>raw-telemetry<br/>3 partitions"]
        DEV --> PROD -->|key = device_id| RAW
    end

    subgraph STREAM1["Streaming Stage 1 — Canonicalization"]
        DEDUP["Spark Dedup<br/>10-min watermark<br/>dropDuplicatesWithinWatermark(event_id)"]
        CLEAN["Kafka Topic<br/>clean-telemetry<br/>3 partitions"]
        RAW --> DEDUP --> CLEAN
        DEDUP -.-> DCP[("spark-dedup-checkpoint")]
    end

    subgraph STREAM2["Streaming Stage 2 — Features"]
        FEAT["Spark Features<br/>5-min window / 1-min slide<br/>rule-based anomaly detection"]
        CLEAN --> FEAT
        FEAT -.-> FCP[("spark-features-checkpoint")]
    end

    subgraph STORE["Durable Sink"]
        PG["PostgreSQL"]
        WF["window_features<br/>PK: window_start + window_end + device_id"]
        PB["processed_stream_batches<br/>PK: query_name + batch_id"]
        FEAT -->|foreachBatch| PG
        PG --> WF
        PG --> PB
    end

    subgraph OBS["Observability"]
        JMX["Kafka JMX Exporter<br/>:7071/metrics"]
        PROM["Prometheus<br/>:9090"]
        GRAF["Grafana<br/>:3000"]

        DEDUP -. Spark metrics .-> PROM
        FEAT -. Spark metrics .-> PROM
        RAW -. broker/topic metrics .-> JMX
        CLEAN -. broker/topic metrics .-> JMX
        JMX --> PROM
        PROM --> GRAF
    end
```

## Reliability model

```mermaid
flowchart LR
    K["Kafka offsets / topic history"]
    C["Spark checkpoint"]
    B["processed_stream_batches"]
    U["PostgreSQL UPSERT"]
    DB["Durable feature state"]

    K --> C
    C -->|"resume stream + restore state"| B
    B -->|"skip replayed batch"| U
    U -->|"insert or update logical window"| DB
```

The three mechanisms have separate responsibilities:

```text
Spark checkpoint
→ tracks streaming progress and state

processed_stream_batches
→ prevents a replayed Spark micro-batch from being committed twice

PostgreSQL UPSERT
→ ensures one logical row per device/window while active windows are updated
```

## Monitoring model

```mermaid
flowchart LR
    P["Producer"]
    KR["Kafka raw"]
    D["Spark Dedup"]
    KC["Kafka clean"]
    F["Spark Features"]
    PG["PostgreSQL"]

    P --> KR --> D --> KC --> F --> PG

    KR -. "messages / bytes / partition health" .-> M["Prometheus + Grafana"]
    D  -. "input / processing / latency / state" .-> M
    KC -. "messages / bytes / partition health" .-> M
    F  -. "input / processing / latency / state" .-> M
```

This gives visibility into **where** a slowdown occurs rather than only showing whether a container is alive.
