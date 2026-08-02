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
ALTER TABLE users_basic ADD COLUMN age TINYINT DEFAULT '25';
ALTER TABLE users_basic ADD COLUMN score SMALLINT DEFAULT '100';
ALTER TABLE users_basic ADD COLUMN salary INT DEFAULT '50000';
ALTER TABLE users_basic ADD COLUMN revenue BIGINT DEFAULT '1000000';
ALTER TABLE users_basic ADD COLUMN rating FLOAT DEFAULT '4.5';
ALTER TABLE users_basic ADD COLUMN percentage DOUBLE DEFAULT '95.5';
INSERT INTO users_basic VALUES (4, 'david', 30, 200, 60000, 2000000, 3.8, 88.9);
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
ALTER TABLE products_with_key ADD COLUMN price1 DOUBLE DEFAULT '99.99';
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    price DOUBLE DEFAULT '0.0',
    discount FLOAT DEFAULT '0.0',
    amount BIGINT DEFAULT '0'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (4, 'monitor', 2);
INSERT INTO orders_column_mode (order_id, product_name, price) VALUES (5, 'keyboard', 299.99);
ALTER TABLE orders_column_mode ADD COLUMN tax_rate DOUBLE DEFAULT '0.08';
INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (7, 'headset', 3);
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    age TINYINT DEFAULT '18',
    score INT DEFAULT '0',
    balance BIGINT DEFAULT '1000'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
INSERT INTO users_pk_table (user_id, username, score) VALUES (1, 'alice_updated', 100);
INSERT INTO users_pk_table (user_id, username, age) VALUES (2, 'bob_updated', 25);
ALTER TABLE users_pk_table ADD COLUMN credit_limit BIGINT DEFAULT '5000';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (1, 'alice_v2', 10000);
CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN event_count INT DEFAULT '1';
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
CREATE TABLE edge_case_numerics (
    id INT NOT NULL,
    tiny_val TINYINT DEFAULT '127',
    small_val SMALLINT DEFAULT '32767',
    int_val INT DEFAULT '2147483647',
    big_val BIGINT DEFAULT '9223372036854775807',
    float_val FLOAT DEFAULT '3.14159',
    double_val DOUBLE DEFAULT '2.718281828',
    zero_val INT DEFAULT '0',
    negative_val INT DEFAULT '-100'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_numerics (id) VALUES (1), (2), (3);
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    total_quantity BIGINT SUM DEFAULT '0',
    total_revenue DOUBLE SUM DEFAULT '0.0',
    max_price DOUBLE MAX DEFAULT '0.0',
    min_price DOUBLE MIN DEFAULT '999999.99'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN avg_discount DOUBLE REPLACE DEFAULT '0.05';
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    stock_quantity INT DEFAULT '100',
    reorder_point INT DEFAULT '20'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN max_stock INT DEFAULT '1000';