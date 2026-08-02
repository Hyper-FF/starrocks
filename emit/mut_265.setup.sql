CREATE TABLE `t_decimal_overflow` (
  `c_id` int(11) NOT NULL,
  `c_d32` decimal32(9,3) NOT NULL,
  `c_d64` decimal64(18,5) NOT NULL,
  `c_d128` decimal128(38,7) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `avg_test` (
  `c0` bigint NULL COMMENT "",
  `c1` array<int> NULL COMMENT "",
  `c2` bigint NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 5
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO `t_decimal_overflow` (c_id, c_d32, c_d64, c_d128) values
   (1, 999999.99, 9999999999999.99999, 9999999999999999999999999999999.9999999),
   (2, -999999.99, -9999999999999.99999, -9999999999999999999999999999999.9999999);
insert into avg_test values (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 123456789, 1);
insert into avg_test values (1, [11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 123456789, 1);