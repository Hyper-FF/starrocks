CREATE TABLE `pri_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
PRIMARY KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
ORDER BY (k2, k1, k2)
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
CREATE TABLE `pri_test` (
    `k1` int(11) NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` bigint REPLACE NULL COMMENT "",
    `v2` bigint REPLACE NULL COMMENT "",
    `v3` bigint REPLACE NULL COMMENT ""
)
PRIMARY KEY(k1, k2)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
ORDER BY (k2, k1)
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
insert into pri_test values (1,3,2,10,9),(2,2,2,9,7),(3,1,2,8,8);
insert into pri_test values (1,2,2,10,9),(2,3,2,9,7),(2,1,2,8,8);
alter table pri_test order by (k2,v1,v1);
alter table pri_test order by (k2,v1);
alter table pri_test order by (k2,k1);