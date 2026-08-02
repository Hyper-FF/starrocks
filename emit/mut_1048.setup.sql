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
DISTRIBUTED BY HASH(`idx`) BUCKETS 640
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util select row_number() over() as idx from __row_util_base;
CREATE TABLE t1 (
    s1 string NULL,
    k1 bigint NULL,
    s2 string NULL,
    int1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`s1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 6
PROPERTIES (
    "replication_num" = "1"
);
insert into t1
select 
lpad(cast(idx % 2157 as string), 10, '0') as s1,
idx,
lpad(cast(idx % 29737 as string), 32, '0') as s2,
idx
from __row_util where idx <= 163840000;