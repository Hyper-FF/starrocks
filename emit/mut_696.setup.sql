CREATE TABLE `common_duplicate` (
  `c0` int(11) NOT NULL COMMENT "",
  `c1` int(11) NOT NULL COMMENT "",
  `c2` varchar(500) NOT NULL COMMENT "",
  `c3` varchar(500) NULL COMMENT "",
  `c4` float NULL COMMENT "",
  `c5` decimal128(38, 20) NOT NULL COMMENT "",
  `c6` date NOT NULL COMMENT "",
  INDEX index1_c0 (`c0`) USING BITMAP
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`, `c2`)
COMMENT "OLAP"
PARTITION BY RANGE(`c1`)
(PARTITION p0 VALUES [("0"), ("500000")),
PARTITION p500000 VALUES [("500000"), ("1000000")))
DISTRIBUTED BY HASH(`c0`, `c1`, `c2`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"bloom_filter_columns" = "c3",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into common_duplicate values (1, 1, 'a', 'b', 1.23, 3.33, '2021-11-11'),(2, 600000, 'aa', 'bb', 2.23, 4.33, '2022-12-11'),(3, 400000, 'aaa', 'bbb', 3.23, 5.33, '2022-06-11');