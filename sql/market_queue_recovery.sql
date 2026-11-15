-- Additive operational migration; never resets queues or historical captures.
ALTER TABLE market_ops.host_backoff
    ADD COLUMN IF NOT EXISTS consecutive_signals INTEGER NOT NULL DEFAULT 0;
