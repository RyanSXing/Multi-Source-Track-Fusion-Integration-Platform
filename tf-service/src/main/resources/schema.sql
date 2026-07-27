CREATE TABLE IF NOT EXISTS track_history (
    id BIGSERIAL PRIMARY KEY,
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

CREATE INDEX IF NOT EXISTS track_history_track_time
    ON track_history (track_id, state_at);
