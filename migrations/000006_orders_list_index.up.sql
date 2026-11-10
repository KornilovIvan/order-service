CREATE INDEX orders_customer_status_created_idx
    ON orders (customer_id, status, created_at DESC);
