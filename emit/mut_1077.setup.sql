CREATE TABLE `js1` (
  `k1` bigint(20) NULL COMMENT "",
  `j1` json NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
insert into js1 values (9, parse_json('{"k1": "v91", "k2": "v92", "k3": "v93", "k4": "v94"}'));
drop table js1;