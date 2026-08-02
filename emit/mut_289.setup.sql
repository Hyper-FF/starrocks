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
ALTER TABLE users_basic ADD COLUMN role VARCHAR(20) DEFAULT 'user';
ALTER TABLE users_basic ADD COLUMN country CHAR(2) DEFAULT 'US';
ALTER TABLE users_basic ADD COLUMN notes STRING DEFAULT 'no notes';
INSERT INTO users_basic VALUES (4, 'david@example.com', 'inactive', 'admin', 'CN', 'important user');
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
ALTER TABLE products_with_key ADD COLUMN brand VARCHAR(50) DEFAULT 'no brand';
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    payment_method VARCHAR(20) DEFAULT 'cash',
    shipping_address STRING DEFAULT 'not specified',
    notes VARCHAR(200) DEFAULT ''
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (4, 'david', 'shipped');
INSERT INTO orders_column_mode (order_id, customer_name, payment_method) VALUES (5, 'eve', 'credit_card');
ALTER TABLE orders_column_mode ADD COLUMN tracking_number VARCHAR(50) DEFAULT 'N/A';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
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
INSERT INTO users_pk_table (user_id, username, status) VALUES (1, 'alice_updated', 'premium');
INSERT INTO users_pk_table (user_id, username, role) VALUES (2, 'bob_updated', 'admin');
ALTER TABLE users_pk_table ADD COLUMN email VARCHAR(100) DEFAULT 'unknown@example.com';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, email) VALUES (1, 'alice_v2', 'alice@example.com');
CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN severity VARCHAR(20) DEFAULT 'INFO';
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
CREATE TABLE edge_case_strings (
    id INT NOT NULL,
    varchar_short VARCHAR(10) DEFAULT 'test',
    varchar_long VARCHAR(255) DEFAULT 'This is a longer default value for testing',
    char_fixed CHAR(5) DEFAULT 'ABCDE',
    string_type STRING DEFAULT 'String type default',
    empty_varchar VARCHAR(50) DEFAULT '',
    special_chars VARCHAR(100) DEFAULT 'Special: @#$%^&*()',
    unicode_str VARCHAR(100) DEFAULT '测试中文'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_strings (id) VALUES (1), (2), (3);
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_status VARCHAR(50) REPLACE DEFAULT 'unknown',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN last_updated_by VARCHAR(50) REPLACE DEFAULT 'system';
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    location VARCHAR(50) DEFAULT 'warehouse',
    supplier VARCHAR(50) DEFAULT 'default supplier'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN barcode VARCHAR(50) DEFAULT 'NO_BARCODE';