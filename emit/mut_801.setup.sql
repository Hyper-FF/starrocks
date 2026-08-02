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
    "replication_num" = "1"
);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 2, 2, 2, 2, 2, 2.0, 2.0, 2.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 3, 3, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 4, 4, 4, 4, 4, 4.0, 4.0, 4.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 5, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 6, 7, 2, 2, 2, 2.0, 2.0, 2.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 7, 8, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 8, 10, 4, 4, 4, 4.0, 4.0, 4.0);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0);
insert into duplicate_tbl values 
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0);