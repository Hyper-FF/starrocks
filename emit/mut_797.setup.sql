CREATE TABLE `duplicate_tbl` (
    `k1` date NULL COMMENT "",   
    `k2` datetime NULL COMMENT "",   
    `k3` char(20) NULL COMMENT "",   
    `k4` varchar(20) NULL COMMENT "",   
    `k5` boolean NULL COMMENT "",   
    `k6` tinyint(4) NULL COMMENT "",   
    `k7` smallint(6) NULL COMMENT "",   
    `k8` int(11) NULL COMMENT "",   
    `k9` bigint(20) NULL COMMENT "",   
    `k10` largeint(40) NULL COMMENT "",   
    `k11` float NULL COMMENT "",   
    `k12` double NULL COMMENT "",   
    `k13` decimal128(27, 9) NULL COMMENT "",   
    INDEX idx1 (`k6`) USING BITMAP 
) 
ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) 
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1", 
    "enable_persistent_index" = "true", 
    "replicated_storage" = "true", 
    "compression" = "LZ4" 
);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0);
drop table if exists case_when_tbl1;
CREATE TABLE case_when_tbl1 (
    k1 INT,
    k2 char(20))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1);
insert into case_when_tbl1 values (1,'xian'), (2, 'beijing'), (3, 'hangzhou');
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0);
drop table if exists case_when_tbl1;
CREATE TABLE IF NOT EXISTS test_base_table1
(
    `col0`             int(11) NULL,
    `col2`           datetime NULL,
    `col3`         varchar(32) NULL,
    `id`               bigint(20) NULL,
    `col1`           bigint(20) NULL
) DUPLICATE KEY(col0, col2, col3)
  PARTITION BY RANGE(col2)(
  START ("2022-04-17") END ("2022-05-01") EVERY (INTERVAL 1 day))
  DISTRIBUTED BY HASH(col0)
  PROPERTIES
(
    "replication_num" = "1"
);
INSERT INTO test_base_table1 (col0, col2, col3, id, col1) VALUES (123456789, '2022-04-30 12:00:00', 'Guangdong', 1, 10001);
INSERT INTO test_base_table1 (col0, col2, col3) VALUES (987654321, '2022-04-30 13:00:00', 'Fujian');
drop table test_base_table1;