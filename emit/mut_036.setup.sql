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
CREATE TABLE t1 (
  k1 bigint NULL,
  c1 bigint NULL,
  c2 int NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 select idx, idx % 10, idx % 2 from __row_util;
insert into t1 select idx, idx % 10, null from __row_util order by idx limit 1000;
insert into t1 select idx, idx % 10, null from __row_util;
CREATE TABLE t2 (
  k1 bigint NULL,
  c1 bigint NULL,
  c2 int NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);
insert into t2 select idx, null, null from __row_util;