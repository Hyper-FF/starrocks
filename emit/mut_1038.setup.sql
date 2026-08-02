CREATE TABLE __row_util_base (
  k1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util_base select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
CREATE TABLE __row_util (
  idx bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util select row_number() over() as idx from __row_util_base;
CREATE TABLE `t1` (
  `idx` bigint(20) NULL COMMENT "",
  `c1` bigint(20) NULL COMMENT "",
  `c2` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
"replication_num" = "1"
);
insert into t1 select idx, 1, 1 from __row_util;
insert into t1 select idx, 2, 2 from __row_util where idx < 160000 - 10;
insert into t1 select idx, 3, 3 from __row_util where idx < 160000 - 10;
insert into t1 select idx, 4, 4 from __row_util where idx < 160000 - 10;
insert into t1 select idx, 5, 5 from __row_util where idx < 160000 - 10;
insert into t1 select idx, 6, 6 from __row_util where idx < 160000 - 10;
insert into t1 select idx, idx, idx  from __row_util where idx > 4;