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
    c_int int NULL,
    c_bigint bigint NULL,
    c_str4 STRING NULL,
    c_str8 STRING NULL,
    c_str16 STRING NULL,
    c_str32 STRING NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 
select
    idx,
    idx,
    idx,
    substr(lpad(idx, 4, '-'), 1, 4),
    substr(lpad(idx, 8, '-'), 1, 8),
    substr(lpad(idx, 16, '-'), 1, 16),
    substr(lpad(idx, 32, '-'), 1, 32)
from __row_util where idx <= 10000;
insert into t1 (k1) select 1001;
CREATE TABLE t2 (
    k1 bigint NULL,
    c_int int NULL,
    c_bigint bigint NULL,
    c_str4 STRING NULL,
    c_str8 STRING NULL,
    c_str16 STRING NULL,
    c_str32 STRING NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into t2 select idx, t1.c_int, t1.c_bigint, t1.c_str4, t1.c_str8, t1.c_str16, t1.c_str32 from __row_util join t1 on idx % 1000 = t1.k1;
insert into t2 (k1) select 320000;
insert into t2 select idx, idx, idx, uuid(), uuid(), uuid(), uuid() from __row_util where idx <= 10000;
insert into t2
select
    320001,
    320001,
    320001,
    '\0\0\0\0',
    '\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0';
insert into t1
select
    320001,
    320001,
    320001,
    '\0\0\0\0',
    '\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0';