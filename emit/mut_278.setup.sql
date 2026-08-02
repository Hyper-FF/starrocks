CREATE TABLE users_basic (
    id INT NOT NULL,
    email VARCHAR(100)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_basic VALUES 
    (1, 'alice@example.com'),
    (2, 'bob@example.com'),
    (3, 'charlie@example.com');
ALTER TABLE users_basic ADD COLUMN status VARCHAR(20) DEFAULT 'active';
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
ALTER TABLE products_with_key ADD COLUMN brand VARCHAR(50) DEFAULT 'no brand';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
INSERT INTO orders_column_mode (order_id, customer_name, payment_method) VALUES (5, 'eve', 'credit_card');
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (7, 'grace', 'delivered');
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    role VARCHAR(20) DEFAULT 'member',
    bio STRING DEFAULT 'No bio available'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
INSERT INTO users_pk_table (user_id, username, role) VALUES (2, 'bob_updated', 'admin');
INSERT INTO users_pk_table (user_id, username, email) VALUES (1, 'alice_v2', 'alice@example.com');
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN severity VARCHAR(20) DEFAULT 'INFO';
INSERT INTO edge_case_strings (id) VALUES (1), (2), (3);
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN last_updated_by VARCHAR(50) REPLACE DEFAULT 'system';
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN barcode VARCHAR(50) DEFAULT 'NO_BARCODE';