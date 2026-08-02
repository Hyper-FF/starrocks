CREATE TABLE `t0` (
  `region` varchar(128) NOT NULL COMMENT "",
  `order_date` date NOT NULL COMMENT "",
  `income` decimal(7, 0) NOT NULL COMMENT "",
  `ship_mode` int NOT NULL COMMENT "",
  `ship_code` int) ENGINE=OLAP
DUPLICATE KEY(`region`, `order_date`)
COMMENT "OLAP"
PARTITION BY RANGE(`order_date`)
(PARTITION p20220101 VALUES [("2022-01-01"), ("2022-01-02")),
PARTITION p20220102 VALUES [("2022-01-02"), ("2022-01-03")),
PARTITION p20220103 VALUES [("2022-01-03"), ("2022-01-05")))
DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
INSERT INTO `t0` (`region`, `order_date`, `income`, `ship_mode`, `ship_code`) VALUES
('USA', '2022-01-01', 12345, 50, 1),
('CHINA', '2022-01-02', 54321, 51, 4),
('JAPAN', '2022-01-03', 67890, 610, 6),
('UK', '2022-01-04', 98765, 75, 2),
('AUS', '2022-01-01', 23456, 25, 18),
('AFRICA', '2022-01-02', 87654, 125, 7),
('USA', '2022-01-03', 54321, 75, null),
('CHINA', '2022-01-04', 12345, 100, 3),
('JAPAN', '2022-01-01', 67890, 64, 10),
('UK', '2022-01-02', 54321, 25, 5),
('AUS', '2022-01-03', 98765, 150, 15),
('AFRICA', '2022-01-04', 23456, 75, null),
('USA', '2022-01-01', 87654, 125, 2),
('CHINA', '2022-01-02', 54321, 175, 12),
('JAPAN', '2022-01-03', 12345, 100, 3),
('UK', '2022-01-04', 67890, 50, 10),
('AUS', '2022-01-01', 54321, 25, 5),
('AFRICA', '2022-01-02', 98765, 150, 15),
('USA', '2022-01-03', 23456, 75, 18),
('CHINA', '2022-01-04', 87654, 125, 7),
('JAPAN', '2022-01-01', 54321, 175, 12),
('UK', '2022-01-02', 12345, 86, 3),
('AUS', '2022-01-03', 67890, 50, 10),
('AFRICA', '2022-01-04', 54321, 25, 95),
('USA', '2022-01-01', 98765, 150, 55),
('CHINA', '2022-01-02', 23456, 75, 88),
('JAPAN', '2022-01-03', 87654, 125, 67),
('UK', '2022-01-04', 54321, 82, 72),
('AUS', '2022-01-01', 12345, 90, 35),
('AFRICA', '2022-01-02', 67890, 50, 100),
('USA', '2022-01-03', 54321, 25, 5),
('CHINA', '2022-01-04', 98765, 150, 15),
('JAPAN', '2022-01-01', 23456, 75, null);
CREATE TABLE `ads_opt_t_goods_odd_important_kd_di` (
  `region` varchar(128) NOT NULL COMMENT "",
  `handed_dept_name` int NOT NULL COMMENT "")
ENGINE=OLAP
DUPLICATE KEY(`region`)
COMMENT "OLAP"
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);