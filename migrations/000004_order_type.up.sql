ALTER TABLE orders ADD COLUMN order_type text NOT NULL DEFAULT 'regular';
ALTER TABLE orders ADD CONSTRAINT orders_type_check CHECK (order_type IN ('regular', 'preorder'));
ALTER TABLE orders ADD COLUMN delivery_date date NULL;
