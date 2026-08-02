CREATE TABLE `t0` (
  `c0` largeint(40) NULL COMMENT "",
  `c1` largeint(40) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1, 300000));