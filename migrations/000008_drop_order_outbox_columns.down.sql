ALTER TABLE orders ADD COLUMN idempotency_key uuid NULL;
ALTER TABLE orders ADD COLUMN published_at timestamptz NULL;
ALTER TABLE orders ADD COLUMN correlation_id text NULL;
