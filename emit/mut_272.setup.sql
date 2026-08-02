CREATE TABLE users_basic (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_basic VALUES 
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie');
ALTER TABLE users_basic ADD COLUMN birth_date DATE DEFAULT '2000-01-01';
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
ALTER TABLE products_with_key 
    ADD COLUMN launch_date DATE DEFAULT '2024-01-01',
    ADD COLUMN last_updated DATETIME DEFAULT '2024-12-17 00:00:00';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
INSERT INTO orders_column_mode (order_id, customer_name, created_at) VALUES (5, 'eve', '2024-07-20 10:30:00');
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (7, 'grace', '2024-08-01');
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    registered_date DATE DEFAULT '2024-01-01',
    last_login DATETIME DEFAULT '2024-01-01 00:00:00',
    birth_date DATE DEFAULT '2000-01-01'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
INSERT INTO users_pk_table (user_id, username, last_login) VALUES (2, 'bob_updated', '2024-12-17 10:30:00');
INSERT INTO users_pk_table (user_id, username, account_expires) VALUES (1, 'alice_v2', '2026-06-30');
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN event_date DATE DEFAULT '2024-01-01';
INSERT INTO edge_case_dates (id) VALUES (1), (2), (3);
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN first_sale_date DATE REPLACE DEFAULT '2023-01-01';
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN expiry_date DATE DEFAULT '2025-12-31';