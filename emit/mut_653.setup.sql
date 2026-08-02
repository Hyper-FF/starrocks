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
    c_bool boolean,
    c_bool_null boolean NULL,
    c_tinyint tinyint,
    c_tinyint_null tinyint NULL,
    c_smallint smallint,
    c_smallint_null smallint NULL,
    c_int int,
    c_int_null int NULL,
    c_bigint bigint,
    c_bigint_null bigint NULL,
    c_float float,
    c_float_null float NULL,
    c_double double,
    c_double_null double NULL,
    c_date date,
    c_date_null date NULL,
    c_datetime datetime,
    c_datetime_null datetime NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 
select
    idx,
    idx % 2 = 0,
    if (idx % 7 = 0, idx % 2 = 0, null),
    idx % 128,
    if (idx % 12 = 0, idx % 128, null),
    idx % 32768,
    if (idx % 13 = 0, idx % 32768, null),
    idx % 2147483648,
    if (idx % 14 = 0, idx % 2147483648, null),
    idx,
    if (idx % 15 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    date_add('2023-01-01', idx % 365),
    if (idx % 17 = 0, date_add('2023-01-01', idx % 365), null),
    date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400),
    if (idx % 18 = 0, date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400), null)
from __row_util;