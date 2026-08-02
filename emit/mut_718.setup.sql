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
    k1 bigint NULL,
    low1 string NULL,
    low2 string NULL,
    low3 string NULL,
    str1 string NULL,
    low_char1 CHAR(40) NULL,
    str_char1 CHAR(40) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 select 
    idx, 
    concat('common-low1-', idx % 100), concat('common-low2-', idx % 200), concat('common-low3-', idx % 200), 
    concat('common-str1-', idx),
    concat('common-low_char1-', idx % 100), concat('common-str_char1-', idx)
from __row_util;
insert into t1 select 
    idx, 
    concat('t1-low1-', idx % 50), concat('t1-low2-', idx % 20), concat('t1-low3-', idx % 50), 
    concat('t1-str1-', idx),
    concat('t1-low_char1-', idx % 50), concat('t1-str_char1-', idx)
from __row_util;
insert into t1(k1) select 10000000;
CREATE TABLE t2 (
    k1 bigint NULL,
    low1 string NULL,
    low2 string NULL,
    low3 string NULL,
    str1 string NULL,
    low_char1 CHAR(40) NULL,
    str_char1 CHAR(40) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into t2 select 
    idx, 
    concat('common-low1-', idx % 100), concat('common-low2-', idx % 200), concat('common-low3-', idx % 200), 
    concat('common-str1-', idx),
    concat('common-low_char1-', idx % 100), concat('common-str_char1-', idx)
from __row_util where idx <= 1000;
insert into t2 select 
    idx + 1000, 
    concat('t2-low1-', idx % 50), concat('t2-low2-', idx % 20), concat('t2-low3-', idx % 50), 
    concat('t2-str1-', idx), 
    concat('t2-low_char1-', idx % 50), concat('t2-str_char1-', idx) 
from __row_util where idx <= 1000;
insert into t2(k1) select 10000000;