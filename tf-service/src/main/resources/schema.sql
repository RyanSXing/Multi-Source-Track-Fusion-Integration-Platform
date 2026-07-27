CREATE TABLE IF NOT EXISTS track_history (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(36),
    track_id BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    state_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL,
    lat_deg DOUBLE PRECISION NOT NULL,
    lon_deg DOUBLE PRECISION NOT NULL,
    alt_meters DOUBLE PRECISION,
    east_velocity_mps DOUBLE PRECISION NOT NULL,
    north_velocity_mps DOUBLE PRECISION NOT NULL,
    hit_count INTEGER NOT NULL,
    consecutive_misses INTEGER NOT NULL,
    contributors_json TEXT NOT NULL
);

ALTER TABLE track_history
    ADD COLUMN IF NOT EXISTS session_id VARCHAR(36);

CREATE INDEX IF NOT EXISTS track_history_track_time
    ON track_history (track_id, state_at);

CREATE TABLE IF NOT EXISTS processed_kafka_records (
    id VARCHAR(256) PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    partition INTEGER NOT NULL,
    offset_value BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    outcome VARCHAR(16) NOT NULL,
    detection_json TEXT
);

ALTER TABLE processed_kafka_records
    ADD COLUMN IF NOT EXISTS detection_json TEXT;

CREATE TABLE IF NOT EXISTS fusion_leadership (
    name VARCHAR(64) PRIMARY KEY,
    epoch BIGINT NOT NULL,
    session_id VARCHAR(36)
);

INSERT INTO fusion_leadership (name, epoch, session_id)
VALUES ('fusion', 0, NULL)
ON CONFLICT (name) DO NOTHING;
