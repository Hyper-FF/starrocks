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
  c_int_1_seq bigint NULL,
  c_int_2_seq bigint NULL,
  c_str_1_seq String NULL,
  c_str_2_seq String NULL,
  c_str_3_low1 String NULL,
  c_str_4_low2 String NULL,
  c_str_5_low_non_null1 String NOT NULL,
  c_str_6_low_non_null2 String NOT NULL,
  c_str_7_seq_non_null1 String NOT NULL,
  c_str_8_seq_non_null2 String NOT NULL,
  c_date_1_seq date NULL,
  c_date_2_seq date NULL,
  c_datetime_1_seq datetime NULL,
  c_datetime_2_seq datetime NULL,
  INDEX c_int_1_seq_bitmap_index (c_int_1_seq) USING BITMAP,
  INDEX c_str_1_seq_bitmap_index (c_str_1_seq) USING BITMAP,
  INDEX c_str_3_low1_bitmap_index (c_str_3_low1) USING BITMAP,
  INDEX c_str_5_low_non_null1_bitmap_index (c_str_5_low_non_null1) USING BITMAP,
  INDEX c_str_7_seq_non_null1_bitmap_index (c_str_7_seq_non_null1) USING BITMAP,
  INDEX c_date_1_seq_bitmap_index (c_date_1_seq) USING BITMAP,
  INDEX c_datetime_1_seq_bitmap_index (c_datetime_1_seq) USING BITMAP
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "bloom_filter_columns" = "c_int_1_seq,c_str_1_seq,c_str_3_low1,c_str_5_low_non_null1,c_str_7_seq_non_null1,c_date_1_seq,c_datetime_1_seq"
);
insert into t1
select 
    idx,
    idx + 1280000,
    idx + 1280000*2,
    concat('abc1-', idx),
    concat('abc2-', idx),
    case when idx % 3 = 0 then 'a1' when idx % 3 = 1 then 'b1' else 'c1' end,
    case when idx < 1280000/4 then 'a2' when idx < 1280000/4*2 then 'b2' when idx < 1280000/4*3 then 'c2' else 'd2' end,
    case when idx % 3 = 0 then 'a1' when idx % 3 = 1 then 'b1' else 'c1' end,
    case when idx < 1280000/4 then 'a2' when idx < 1280000/4*2 then 'b2' when idx < 1280000/4*3 then 'c2' else 'd2' end,
    concat('abc1-', idx),
    concat('abc2-', idx),
    cast(date_sub('2023-11-02', interval cast(idx % 100 as int) day) as date),
    cast(date_sub('2023-11-02', interval cast(idx % 1000 as int) day) as date),
    date_sub('2023-11-02', interval cast(idx % 100 as int) second),
    date_sub('2023-11-02', interval cast(idx % 1000 as int) second)
from __row_util;
insert into t1 (k1, c_str_5_low_non_null1, c_str_6_low_non_null2, c_str_7_seq_non_null1, c_str_8_seq_non_null2) select null, '<null>', '<null>', '<null>', '<null>';
insert into t1 (k1, c_str_5_low_non_null1, c_str_6_low_non_null2, c_str_7_seq_non_null1, c_str_8_seq_non_null2) select null, '<null>', '<null>', '<null>', '<null>';
ALTER TABLE t1 COMPACT;
create view __profile(idx, k, v) as 
select 1, 2, 3;