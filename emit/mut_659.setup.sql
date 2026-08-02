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
    c_int int,
    c_int_null int NULL,
    c_bigint bigint,
    c_bigint_null bigint NULL,
    c_largeint bigint,
    c_largeint_null bigint NULL,
    c_double double,
    c_double_null double NULL,
    c_string STRING,
    c_string_null STRING NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 
select
    idx,
    idx, 
    if (idx % 13 = 0, idx, null),
    idx, 
    if (idx % 14 = 0, idx, null),
    idx, 
    if (idx % 15 = 0, idx, null),
    idx, 
    if (idx % 16 = 0, idx, null),
    concat('str-', idx), 
    if (idx % 17 = 0, concat('str-', idx), null)
from __row_util;