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
ALTER TABLE users_basic ADD COLUMN flag_true BOOLEAN DEFAULT 'true';
INSERT INTO products_with_key VALUES (1, 100), (2, 200), (3, 300);
ALTER TABLE products_with_key ADD COLUMN active BOOLEAN DEFAULT 'true';
INSERT INTO items_type_change VALUES (1, 10, 1), (2, 20, 0);
ALTER TABLE items_type_change ADD COLUMN verified BOOLEAN DEFAULT '1';
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');
INSERT INTO orders_column_mode (order_id, product_name, total_amount) VALUES (5, 'keyboard', 200);
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (7, 'headset', 1);
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT 'true',
    is_verified BOOLEAN DEFAULT 'false',
    credit_score INT DEFAULT '0'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
INSERT INTO users_pk_table (user_id, username, is_active) VALUES (2, 'bob_updated', 0);
INSERT INTO users_pk_table (user_id, username, is_premium) VALUES (1, 'alice_v2', 0);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN is_processed BOOLEAN DEFAULT 'false';
INSERT INTO edge_case_booleans (id) VALUES (1), (2), (3);
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN is_verified BOOLEAN REPLACE DEFAULT 'false';
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN is_discontinued BOOLEAN DEFAULT '0';