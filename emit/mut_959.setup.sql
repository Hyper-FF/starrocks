CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series % 5, generate_series FROM TABLE(generate_series(1,  10000));
CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, generate_series % 5, generate_series FROM TABLE(generate_series(1,  10000));
CREATE TABLE `t1` (
  `k` int DEFAULT NULL,
  `v` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t2` (
  `k` int DEFAULT NULL,
  `v` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));
insert into t2 SELECT generate_series * 2, generate_series FROM TABLE(generate_series(1, 500));
CREATE TABLE `t1` (
  `k` int DEFAULT NULL,
  `v` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t2` (
  `k` int DEFAULT NULL,
  `v` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));
insert into t2 SELECT generate_series * 2, generate_series FROM TABLE(generate_series(1, 500));
CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, 10000 - generate_series, generate_series FROM TABLE(generate_series(1, 10000));
CREATE TABLE `t1` (
  `k` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `t2` (
  `k` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t1 SELECT generate_series FROM TABLE(generate_series(1, 100));
insert into t2 SELECT generate_series FROM TABLE(generate_series(1, 200));
CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1, 10000));
CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1, 1000));