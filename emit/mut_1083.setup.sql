CREATE TABLE __row_util_base (
  k1 bigint NULL
) DISTRIBUTED BY HASH(`k1`) BUCKETS 64
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
) DISTRIBUTED BY HASH(`idx`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util select row_number() over() as idx from __row_util_base;
CREATE TABLE t1 (
    k1 bigint NULL,
    c_int bigint NULL,
    c_json JSON NULL,
    c_array_int ARRAY<INT> NULL,
    c_map MAP<INT, INT> NULL,
    c_struct STRUCT<k1 INT, k2 INT> NULL
) DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1
SELECT
    idx,
    idx,
    json_object('k1', idx, 'k2', idx + 1),
    [idx, idx + 1, idx + 2, idx + 3],
    map{0: idx, 1: idx + 1, 2: idx + 2},
    struct(idx, idx + 1)
FROM __row_util;
INSERT INTO t1 (k1) SELECT idx from __row_util order by idx limit 10000;