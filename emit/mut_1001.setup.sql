CREATE TABLE `t1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` varchar(20) NULL COMMENT ""
) DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE `t1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` varchar(20) NULL COMMENT ""
) DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
create view v as select * from t1;
CREATE TABLE `t1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` varchar(20) NULL COMMENT ""
) DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
create materialized view mv DISTRIBUTED BY HASH(`v1`) BUCKETS 12 REFRESH ASYNC as select v1,v2,v3 from t1;