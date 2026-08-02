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
ALTER TABLE users_basic ADD COLUMN balance DECIMAL(10, 2) DEFAULT '1000.50';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
INSERT INTO orders_column_mode (order_id, customer_name, tax_rate) VALUES (5, 'eve', 0.1000);
INSERT INTO orders_column_mode (order_id, customer_name, unit_price) VALUES (7, 'grace', 499.99);
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    account_balance DECIMAL(12, 2) DEFAULT '1000.00',
    credit_limit DECIMAL(10, 2) DEFAULT '5000.00',
    interest_rate DECIMAL(5, 4) DEFAULT '0.0350'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (2, 'bob_updated', 10000.00);
INSERT INTO users_pk_table (user_id, username, reward_points) VALUES (1, 'alice_v2', 500.00);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN processing_time DECIMAL(8, 3) DEFAULT '1.500';
INSERT INTO edge_case_decimals (id) VALUES (1), (2), (3);
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN avg_tax DECIMAL(5, 4) REPLACE DEFAULT '0.0800';
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN markup_rate DECIMAL(5, 2) DEFAULT '20.00';