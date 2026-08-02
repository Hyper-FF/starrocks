CREATE TABLE `t0` (   `v1` bigint NULL COMMENT "",   `v2` bigint NULL COMMENT "",   `v3` bigint NULL ) ENGINE=OLAP DUPLICATE KEY(`v1`, `v2`, v3) DISTRIBUTED BY HASH(`v1`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
CREATE TABLE `t1` (   `v4` bigint NULL COMMENT "",   `v5` bigint NULL COMMENT "",   `v6` bigint NULL ) ENGINE=OLAP DUPLICATE KEY(`v4`, `v5`, v6) DISTRIBUTED BY HASH(`v4`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
CREATE TABLE `t2` (   `v7` bigint NULL COMMENT "",   `v8` bigint NULL COMMENT "",   `v9` bigint NULL ) ENGINE=OLAP DUPLICATE KEY(`v7`, `v8`, v9) DISTRIBUTED BY HASH(`v7`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
CREATE TABLE `t3` (   `v10` bigint NULL COMMENT "",   `v11` bigint NULL COMMENT "",   `v12` bigint NULL ) ENGINE=OLAP DUPLICATE KEY(`v10`, `v11`, v12) DISTRIBUTED BY HASH(`v10`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
CREATE TABLE `test_all_type` (   `t1a` varchar(20) NULL COMMENT "",   `t1b` smallint(6) NULL COMMENT "",   `t1c` int(11) NULL COMMENT "",   `t1d` bigint(20) NULL COMMENT "",   `t1e` float NULL COMMENT "",   `t1f` double NULL COMMENT "",   `t1g` bigint(20) NULL COMMENT "",   `id_datetime` datetime NULL COMMENT "",   `id_date` date NULL COMMENT "",    `id_decimal` decimal(10,2) NULL COMMENT ""  ) ENGINE=OLAP DUPLICATE KEY(`t1a`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t1a`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
CREATE TABLE `test_all_type_not_null` (   `t1a` varchar(20) NOT NULL COMMENT "",   `t1b` smallint(6) NOT NULL COMMENT "",   `t1c` int(11) NOT NULL COMMENT "",   `t1d` bigint(20) NOT NULL COMMENT "",   `t1e` float NOT NULL COMMENT "",   `t1f` double NOT NULL COMMENT "",   `t1g` bigint(20) NOT NULL COMMENT "",   `id_datetime` datetime NOT NULL COMMENT "",   `id_date` date NOT NULL COMMENT "",    `id_decimal` decimal(10,2) NOT NULL COMMENT ""  ) ENGINE=OLAP DUPLICATE KEY(`t1a`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t1a`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "in_memory" = "false" );
insert into t0 values (-10, -10, -10), (0, 0, 0), (1, 1, 1), (2, 2, 2), (10, 10, 10), (20, 20, 20), (75, 75, 75), (511, 511, 511);
insert into t1 select * from t0;
insert into t1 select v1 - 5, v2 + 5, v3 + 10 from t0 order by v1 limit 3;
insert into t2 select * from t0;
insert into t1 select v1 + 5, v2 -10, v3 + 5 from t0 order by v1 limit 3;
insert into t3 select * from t0;
insert into t3 select v1 * 2, v2 + 5, v3 * 3 from t0 order by v1 limit 3;
insert into test_all_type values
('abc', 1, 1, 20, 1.1, 1.1, 1, '2021-01-01 00:00:00', '2021-01-01', 1.2),
('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-01 00:00:00', '2021-01-01', 1.2),
('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('中文', 1, 1, 1, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-01 00:00:00', '2021-04-01', 1.25),
('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-02 00:00:00', '2021-04-02', 1.25),
('abcdefg', 20, 20, 20, 11.2, 11.2, 20, '2021-01-01 00:00:00', '2021-01-01', 2.25),
('中文', 100, 100, 100, 100.01, 100.02, 1, '2021-01-01 00:00:00', '2021-01-01', 100.25),
(null, null, null, null, null, null, null, null, null, null);
insert into test_all_type_not_null values
('ab', 1, -1, 18, 1.1, 1.1, 1, '2022-01-01 00:00:00', '2022-01-01', 1.2),
('abc', 1, 1, 18, 1.1, 1.1, 1, '2021-01-01 00:00:00', '2021-01-01', 1.2),
('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-01 00:00:00', '2021-01-01', 1.2),
('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('中文', 1, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('abc', 1, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('中文', 21, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('中文', 200, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('中文', 1, 1, 1, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-01 00:00:00', '2021-04-01', 1.25),
('abcd', 2, 2, 1, 1.2, 1.2, 10, '2021-04-02 00:00:00', '2021-04-02', 1.25),
('abcdefg', 20, 20, 20, 11.2, 11.2, 20, '2021-01-01 00:00:00', '2021-01-01', 2.25),
('中文', 100, 100, 100, 100.01, 100.02, 1, '2021-01-01 00:00:00', '2021-01-01', 100.25);