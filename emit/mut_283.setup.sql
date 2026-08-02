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
ALTER TABLE users_basic ADD COLUMN created_at DATETIME DEFAULT '2024-01-01 00:00:00';
ALTER TABLE users_basic ADD COLUMN updated_at DATETIME DEFAULT '2024-12-17 10:00:00';
INSERT INTO users_basic VALUES (4, 'david', '1995-05-15', '2024-06-01 12:30:00', '2024-12-18 15:45:30');
CREATE TABLE products_with_key (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
ALTER TABLE products_with_key 
    ADD COLUMN launch_date DATE DEFAULT '2024-01-01',
    ADD COLUMN last_updated DATETIME DEFAULT '2024-12-17 00:00:00';
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    order_date DATE DEFAULT '2024-01-01',
    shipped_date DATE DEFAULT '2024-01-02',
    created_at DATETIME DEFAULT '2024-01-01 00:00:00',
    updated_at DATETIME DEFAULT '2024-01-01 00:00:00'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (4, 'david', '2024-06-15');
INSERT INTO orders_column_mode (order_id, customer_name, created_at) VALUES (5, 'eve', '2024-07-20 10:30:00');
ALTER TABLE orders_column_mode ADD COLUMN delivery_date DATE DEFAULT '2024-01-10';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
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
INSERT INTO users_pk_table (user_id, username, registered_date) VALUES (1, 'alice_updated', '2024-06-01');
INSERT INTO users_pk_table (user_id, username, last_login) VALUES (2, 'bob_updated', '2024-12-17 10:30:00');
ALTER TABLE users_pk_table ADD COLUMN account_expires DATE DEFAULT '2025-12-31';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, account_expires) VALUES (1, 'alice_v2', '2026-06-30');
CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN event_date DATE DEFAULT '2024-01-01';
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
CREATE TABLE edge_case_dates (
    id INT NOT NULL,
    date_early DATE DEFAULT '1970-01-01',
    date_recent DATE DEFAULT '2024-12-17',
    datetime_midnight DATETIME DEFAULT '2024-01-01 00:00:00',
    datetime_noon DATETIME DEFAULT '2024-06-15 12:00:00',
    datetime_full DATETIME DEFAULT '2024-12-17 23:59:59'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_dates (id) VALUES (1), (2), (3);
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_sale_date DATE REPLACE DEFAULT '2024-01-01',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN first_sale_date DATE REPLACE DEFAULT '2023-01-01';
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    added_date DATE DEFAULT '2024-01-01',
    last_checked DATETIME DEFAULT '2024-01-01 00:00:00'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN expiry_date DATE DEFAULT '2025-12-31';