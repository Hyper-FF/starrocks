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
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null date NULL,
  c_date_2_notnull date NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq datetime NULL,
  c_datetime_2_seq datetime NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t2 (
  k1 bigint NULL,
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null date NULL,
  c_date_2_notnull date NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq datetime NULL,
  c_datetime_2_seq datetime NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
CREATE TABLE t3 (
  k1 bigint NULL,
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null date NULL,
  c_date_2_notnull date NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq datetime NULL,
  c_datetime_2_seq datetime NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t2 
SELECT 
    idx,
    idx % 2 = 0,
    idx % 2 = 0,
    idx % 128,
    idx % 128,
    idx % 32768,
    idx % 32768,
    idx % 2147483648,
    idx % 2147483648,
    idx,
    idx,
    cast(date_add('2023-01-01', interval idx day) as date),
    cast(date_add('2023-01-01', interval idx day) as date),
    idx,
    idx,
    concat('str-abc-', idx),
    concat('str-abc-', idx),
    concat('str-abc-', idx % 256),
    concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util
order by idx
limit 100000;
INSERT INTO t2 
SELECT 
    idx,
    NULL,
    idx % 2 = 0,
    NULL,
    idx % 128,
    NULL,
    idx % 32768,
    NULL,
    idx % 2147483648,
    NULL,
    idx,
    NULL,
    cast(date_add('2023-01-01', interval idx day) as date),
    NULL,
    idx,
    NULL,
    concat('str-abc-', idx),
    NULL,
    concat('str-abc-', idx % 256),
    NULL,
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util
order by idx
limit 100000, 10000;
INSERT INTO t3 
SELECT 
    idx,
    idx % 2 = 0,
    idx % 2 = 0,
    idx % 128,
    idx % 128,
    idx % 32768,
    idx % 32768,
    idx * 37 % 2147483648 ,
    idx * 37 % 2147483648,
    idx * 37,
    idx * 37,
    cast(date_add('2023-01-01', interval idx * 37 day) as date),
    cast(date_add('2023-01-01', interval idx * 37 day) as date),
    idx * 37,
    idx * 37,
    concat('str-abc-', idx),
    concat('str-abc-', idx),
    concat('str-abc-', idx % 256),
    concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util
order by idx
limit 100000;
INSERT INTO t3 
SELECT 
    idx,
    NULL,
    idx % 2 = 0,
    NULL,
    idx % 128,
    NULL,
    idx % 32768,
    NULL,
    idx * 37 % 2147483648,
    NULL,
    idx * 37,
    NULL,
    cast(date_add('2023-01-01', interval idx * 37 day) as date),
    NULL,
    idx * 37,
    NULL,
    concat('str-abc-', idx),
    NULL,
    concat('str-abc-', idx % 256),
    NULL,
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util
order by idx
limit 100000, 10000;
INSERT INTO t1
SELECT 
    idx,
    idx % 2 = 0,
    idx % 2 = 0,
    idx % 128,
    idx % 128,
    idx % 32768,
    idx % 32768,
    idx % 2147483648,
    idx % 2147483648,
    idx,
    idx,
    cast(date_add('2023-01-01', interval idx day) as date),
    cast(date_add('2023-01-01', interval idx day) as date),
    idx,
    idx,
    concat('str-abc-', idx),
    concat('str-abc-', idx),
    concat('str-abc-', idx % 256),
    concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util;
INSERT INTO t1
SELECT 
    idx,
    NULL,
    idx % 2 = 0,
    NULL,
    idx % 128,
    NULL,
    idx % 32768,
    NULL,
    idx % 2147483648,
    NULL,
    idx,
    NULL,
    cast(date_add('2023-01-01', interval idx day) as date),
    NULL,
    idx,
    NULL,
    concat('str-abc-', idx),
    NULL,
    concat('str-abc-', idx % 256),
    NULL,
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM __row_util
order by idx
limit 100000, 10000;