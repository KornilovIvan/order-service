ALTER TABLE orders DROP COLUMN idempotency_key;
ALTER TABLE orders DROP COLUMN published_at;
ALTER TABLE orders DROP COLUMN correlation_id;
