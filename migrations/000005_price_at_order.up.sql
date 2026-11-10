ALTER TABLE order_items ADD COLUMN price_at_order numeric(12,2);
UPDATE order_items SET price_at_order = 0 WHERE price_at_order IS NULL;
ALTER TABLE order_items ALTER COLUMN price_at_order SET NOT NULL;
