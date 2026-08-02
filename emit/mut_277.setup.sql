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
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
ALTER TABLE products_with_key ADD COLUMN price1 DOUBLE DEFAULT '99.99';
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');
INSERT INTO orders_column_mode (order_id, product_name, price) VALUES (5, 'keyboard', 299.99);
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
INSERT INTO users_pk_table (user_id, username, age) VALUES (2, 'bob_updated', 25);
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (1, 'alice_v2', 10000);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN event_count INT DEFAULT '1';
INSERT INTO edge_case_numerics (id) VALUES (1), (2), (3);
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN avg_discount DOUBLE REPLACE DEFAULT '0.05';
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN max_stock INT DEFAULT '1000';