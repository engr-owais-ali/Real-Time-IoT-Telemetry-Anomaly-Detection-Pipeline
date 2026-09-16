CREATE TABLE IF NOT EXISTS window_features (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,

    event_count BIGINT NOT NULL,

    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,

    avg_vibration DOUBLE PRECISION,
    max_vibration DOUBLE PRECISION,
    stddev_vibration DOUBLE PRECISION,

    avg_voltage DOUBLE PRECISION,
    min_voltage DOUBLE PRECISION,

    detected_anomaly_count BIGINT NOT NULL,
    injected_anomaly_count BIGINT NOT NULL,

    has_anomaly BOOLEAN NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        window_start,
        window_end,
        device_id
    )
);


CREATE TABLE IF NOT EXISTS processed_stream_batches (
    query_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        query_name,
        batch_id
    )
);