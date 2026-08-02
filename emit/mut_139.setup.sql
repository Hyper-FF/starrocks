CREATE TABLE orders_nulls (
  `order_id` int,
  `user_id` int,           
  `order_time` datetime,   
  `amount` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE prices_nulls (
  `product_id` int,        
  `price_time` datetime,   
  `price` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");
INSERT INTO orders_nulls VALUES
(1, 101, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 12:00:00', 120.00),
(3, 102, '2024-01-02 10:00:00', 130.00),
(4, NULL, '2024-01-01 11:00:00', 90.00),
(5, 101, NULL, 110.00),
(6, 102, NULL, 140.00);
INSERT INTO prices_nulls VALUES
(101, '2024-01-01 09:00:00', 9.50),
(101, '2024-01-01 11:30:00', 10.50),
(101, '2024-01-01 12:30:00', 10.80),
(102, '2024-01-02 08:00:00', 12.00),
(102, '2024-01-02 12:00:00', 12.50),
(NULL, '2024-01-01 08:00:00', 8.88),
(101, NULL, 10.00),
(102, NULL, 12.34);
CREATE TABLE dm_prices_nulls (
  product_id tinyint,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO dm_orders_nulls VALUES
(1, 1,  '2024-01-01 10:00:00', 10.0),
(2, 1,  NULL,                  20.0),
(3, NULL,'2024-01-01 11:00:00', 30.0);
INSERT INTO dm_prices_nulls VALUES
(1, '2024-01-01 09:00:00', 9.0),
(1, NULL,                  9.5),
(NULL,'2024-01-01 08:00:00',8.8);
CREATE TABLE rdm_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO rdm_orders_nulls VALUES
(1, 100, '2024-01-01 10:00:00', 10.0),
(2, 100, NULL,                  20.0),
(3, NULL,'2024-01-01 12:00:00', 30.0);
INSERT INTO rdm_prices_nulls VALUES
(100,'2024-01-01 09:00:00', 9.0),
(100,'2024-01-01 09:30:00', 9.5),
(NULL,'2024-01-01 08:00:00',8.8),
(100, NULL, 9.9);
CREATE TABLE dense_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO dense_prices_nulls
SELECT 
  1600000000 + (generate_series - 1) * 10 + 10,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  9.0 + (generate_series % 100)
FROM TABLE(generate_series(1, 500000));
INSERT INTO dense_orders_nulls VALUES
(1, 1600000010, '2024-01-01 10:00:00', 10.0),
(2, 1600001010, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
CREATE TABLE lc_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
CREATE TABLE lc_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO lc_prices_nulls
SELECT 
  1700000000 + (generate_series - 1) * 1000,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  20.0 + (generate_series % 10)
FROM TABLE(generate_series(1, 5));
INSERT INTO lc_orders_nulls VALUES
(1, 1700000000, '2024-01-01 10:00:00', 10.0),
(2, 1700050000, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
INSERT INTO lca_prices_nulls VALUES (1800000000,'2024-01-01 08:00:00', 30.0);
INSERT INTO lca_orders_nulls VALUES (1,1800000000,'2024-01-01 10:00:00',10.0),(2,NULL,'2024-01-01 11:00:00',20.0),(3,1800000000,NULL,30.0);