CREATE TABLE `test_count_if` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` varchar(65533) NULL COMMENT "",
  `v3` datetime NULL COMMENT "",
  `v4` int null
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
PARTITION BY RANGE(`v3`)
(PARTITION p20220418 VALUES [("2022-04-18 00:00:00"), ("2022-04-19 00:00:00")),
PARTITION p20220419 VALUES [("2022-04-19 00:00:00"), ("2022-04-20 00:00:00")),
PARTITION p20220420 VALUES [("2022-04-20 00:00:00"), ("2022-04-21 00:00:00")),
PARTITION p20220421 VALUES [("2022-04-21 00:00:00"), ("2022-04-22 00:00:00")))
DISTRIBUTED BY HASH(`v1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into test_count_if values('a','a', '2022-04-18 01:01:00', 1);
insert into test_count_if values('a','b', '2022-04-18 02:01:00', NULL);
insert into test_count_if values('a',NULL, '2022-04-18 02:05:00', 1);
insert into test_count_if values('a','b', '2022-04-18 02:15:00', 3);
insert into test_count_if values('a','b', '2022-04-18 03:15:00', 7);
insert into test_count_if values('c',NULL, '2022-04-18 03:45:00', NULL);
insert into test_count_if values('c',NULL, '2022-04-18 03:25:00', 2);
insert into test_count_if values('c','a', '2022-04-18 03:27:00', 3);