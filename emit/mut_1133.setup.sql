CREATE TABLE `agg_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
AGGREGATE KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY (k2)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "true"
);
CREATE TABLE `agg_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
AGGREGATE KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY (k2,v2)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "true"
);
CREATE TABLE `agg_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
AGGREGATE KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY (k2,k1,k1)
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "true"
);
CREATE TABLE `agg_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
AGGREGATE KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY (k2,k1)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "true"
);
insert into agg_test values (1,3,2,10,9),(2,2,2,9,7),(3,1,2,8,8);
insert into agg_test values (1,2,2,10,9),(2,3,2,9,7),(2,1,2,8,8);
alter table agg_test order by (k2,v1);
alter table agg_test order by (k2);
alter table agg_test order by (k1,k2,k2);
alter table agg_test order by (k1,k2);
alter table agg_test modify column k1 bigint key;
alter table agg_test modify column k2 bigint key;
alter table agg_test order by (k2,k1,v2,v1,v3);
drop table agg_test;
CREATE TABLE `agg_test` (
  `k1` bigint(20) NULL COMMENT "",
  `k2` bigint(20) NULL COMMENT "",
  `k3` bigint(20) NULL COMMENT "",
  `k4` bigint(20) NULL COMMENT "",
  `k5` bigint(20) NULL COMMENT "",
  `v1` bigint(20) SUM COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into agg_test values (1,2,3,3,2,1), (1,2,3,4,1,2);
insert into agg_test values (1,2,3,3,2,1), (1,2,3,4,1,2);
alter table agg_test order by (k5,k4,k3,k2,k1);
CREATE TABLE `agg_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
AGGREGATE KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY (k2,k1)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "true"
);
insert into agg_test values (1,3,2,10,9),(2,2,2,9,7),(3,1,2,8,8);
insert into agg_test values (1,2,2,10,9),(2,3,2,9,7),(2,1,2,8,8);
alter table agg_test add rollup r1 (k1,k2,v3,v1);
alter table agg_test order by (k1,k2);
alter table agg_test order by (k2,k1,v2,v1,v3);
drop table agg_test;