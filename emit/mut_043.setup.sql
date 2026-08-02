CREATE TABLE `test_cc` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` varchar(65533) NULL COMMENT "",
  `v3` datetime NULL COMMENT "",
  `v4` int null,
  `v5` decimal(32, 2) null,
  `v6` array<int> null,
  `v7` struct<a bigint(20), b char(20)>  NULL
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
insert into test_cc values('a','a', '2022-04-18 01:01:00', 1, 1.2,  [1, 2, 3], row(1, 'a'));
insert into test_cc values('a','b', '2022-04-18 02:01:00', 2, 1.3,  [2, 1, 3], row(2, 'a'));
insert into test_cc values('a','a', '2022-04-18 02:05:00', 1, 2.3,  [2, 2, 3], row(3, 'a'));
insert into test_cc values('a','b', '2022-04-18 02:15:00', 3, 3.31,  [2, 2, 3], row(4, 'a'));
insert into test_cc values('a','b', '2022-04-18 03:15:00', 1, 100.3,  [3, 1, 3], row(2, 'a'));
insert into test_cc values('c','a', '2022-04-18 03:45:00', 1, 200.3,  [2, 2, 3], row(3, 'a'));
insert into test_cc values('c','a', '2022-04-18 03:25:00', 2, 300.3,  null, row(2, 'a'));
insert into test_cc values('c','a', '2022-04-18 03:27:00', 3, 400.3,  [3, 1, 3], null);