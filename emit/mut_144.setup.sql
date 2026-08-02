CREATE TABLE transactions (
  `txn_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `account_id` varchar(20) NOT NULL,
  `txn_time` datetime NOT NULL,
  `amount` decimal(15,2) NOT NULL,
  `txn_type` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`txn_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE exchange_rates (
  `currency_pair` varchar(10) NOT NULL,
  `rate_time` datetime NOT NULL,
  `exchange_rate` decimal(10,6) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`currency_pair`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE user_profiles (
  `user_id` int(11) NOT NULL,
  `profile_time` datetime NOT NULL,
  `risk_level` varchar(20) NOT NULL,
  `credit_score` int(11) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
CREATE TABLE market_events (
  `event_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `currency` varchar(20) NOT NULL,
  `event_type` varchar(30) NOT NULL,
  `market_impact` decimal(5,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
INSERT INTO transactions VALUES
(1, 101, 'USD', '2024-01-01 10:00:00', 1000.00, 'DEPOSIT'),
(2, 101, 'USD', '2024-01-01 15:30:00', 500.00, 'WITHDRAWAL'),
(3, 102, 'EUR', '2024-01-01 11:00:00', 2000.00, 'DEPOSIT'),
(4, 102, 'EUR', '2024-01-01 16:00:00', 800.00, 'WITHDRAWAL'),
(5, 101, 'USD', '2024-01-02 09:00:00', 1500.00, 'DEPOSIT'),
(6, 102, 'EUR', '2024-01-02 14:00:00', 1200.00, 'WITHDRAWAL');
INSERT INTO exchange_rates VALUES
('USD', '2024-01-01 08:00:00', 0.900000),
('USD', '2024-01-01 14:00:00', 0.950000),
('USD', '2024-01-02 08:00:00', 0.960000),
('EUR', '2024-01-01 08:00:00', 1.100000),
('EUR', '2024-01-01 13:00:00', 1.120000),
('EUR', '2024-01-02 08:00:00', 1.130000);
INSERT INTO user_profiles VALUES
(101, '2024-01-01 09:00:00', 'LOW', 760),
(101, '2024-01-01 11:30:00', 'MEDIUM', 805),
(101, '2024-01-02 08:00:00', 'HIGH', 860),
(102, '2024-01-01 10:00:00', 'LOW', 710),
(102, '2024-01-01 12:30:00', 'MEDIUM', 770),
(102, '2024-01-02 12:00:00', 'HIGH', 810);
INSERT INTO market_events VALUES
(1, '2024-01-01 08:00:00', 'USD', 'MARKET_OPEN', 0.50),
(2, '2024-01-01 12:00:00', 'USD', 'FED_ANNOUNCEMENT', 1.20),
(3, '2024-01-01 16:00:00', 'USD', 'MARKET_CLOSE', -0.30),
(4, '2024-01-02 08:30:00', 'EUR', 'MARKET_OPEN', 0.25);
CREATE TABLE events_dt6 (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_dt6 (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO snapshots_dt6 VALUES
(701,'2024-01-01 10:00:00.100000','S1'),
(701,'2024-01-01 10:00:00.300000','S2'),
(701,'2024-01-01 10:00:00.900000','S3');
INSERT INTO snapshots_dt6 VALUES
(702,'2024-01-01 10:00:00.123456','T1'),
(702,'2024-01-01 10:00:00.223456','T2'),
(702,'2024-01-01 10:00:00.323456','T3');
INSERT INTO events_dt6 VALUES
(1,701,'2024-01-01 10:00:00.350000'), 
(2,701,'2024-01-01 10:00:00.100001'), 
(3,702,'2024-01-01 10:00:00.323455'), 
(4,702,'2024-01-01 10:00:00.500000'), 
(5,702,'2024-01-01 10:00:00.123456'), 
(6,702,'2024-01-01 09:59:59.999999');
CREATE TABLE orders (
  `id` int(11) NULL COMMENT "订单ID",
  `product` varchar(50) NULL COMMENT "产品名称",
  `order_time` datetime NULL COMMENT "订单时间",
  `quantity` int(11) NULL COMMENT "订单数量",
  `max_price` decimal(10, 2) NULL COMMENT "最大价格"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE prices (
  `id` int(11) NULL COMMENT "价格记录ID",
  `product` varchar(50) NULL COMMENT "产品名称",
  `price_time` datetime NULL COMMENT "价格时间",
  `price` decimal(10, 2) NULL COMMENT "价格",
  `volume` int(11) NULL COMMENT "成交量"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
INSERT INTO orders VALUES
(1, 'A', '2024-01-01 10:00:00', 100, 100.00),
(2, 'A', '2024-01-01 11:30:00', 150, 60.00),
(3, 'A', '2024-01-01 14:00:00', 200, 50.00),
(4, 'A', '2024-01-01 16:00:00', 50, 120.00),
(5, 'B', '2024-01-01 09:30:00', 400, 25.00),
(6, 'B', '2024-01-01 12:00:00', 100, 30.00),
(7, 'B', '2024-01-01 15:30:00', 80, 40.00),
(8, 'C', '2024-01-01 13:00:00', 500, 20.00),
(9, 'C', '2024-01-01 17:00:00', 160, 50.00);
INSERT INTO prices VALUES
(1, 'A', '2024-01-01 08:00:00', 80.00, 1000),
(16, 'A', '2024-01-01 09:00:00', 60.00, 1000),
(2, 'A', '2024-01-01 09:30:00', 85.00, 1000),
(3, 'A', '2024-01-01 10:30:00', 53.33, 1500),
(4, 'A', '2024-01-01 12:00:00', 40.00, 1200),
(5, 'A', '2024-01-01 15:00:00', 120.00, 800),
(6, 'A', '2024-01-01 18:00:00', 90.00, 900),
(7, 'B', '2024-01-01 08:30:00', 20.00, 2000),
(8, 'B', '2024-01-01 10:00:00', 30.00, 1800),
(9, 'B', '2024-01-01 11:00:00', 80.00, 1500),
(17, 'B', '2024-01-01 13:00:00', 50.00, 2000),
(10, 'B', '2024-01-01 14:00:00', 100.00, 1400),
(11, 'B', '2024-01-01 16:30:00', 35.00, 1400),
(12, 'C', '2024-01-01 09:00:00', 16.00, 3000),
(13, 'C', '2024-01-01 12:30:00', 18.00, 2800),
(18, 'C', '2024-01-01 15:00:00', 30.00, 3000),
(14, 'C', '2024-01-01 16:00:00', 50.00, 1000),
(15, 'C', '2024-01-01 19:00:00', 45.00, 900);