create table t_reverse(id int, s varchar(65533), ns varchar(65533))
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id)
        BUCKETS 1
        PROPERTIES('replication_num'='1');
insert into t_reverse values
    (1, 'hello', NULL),
    (2, 'abcde', 'edcba'),
    (3, '',      ''),
    (4, '中文字符', NULL),
    (5, 'hello世界', NULL);
drop table t_reverse;
create table t_reverse_types(
    id int,
    c_int int,
    c_bigint bigint,
    c_boolean boolean,
    c_date date,
    c_datetime datetime,
    c_double double,
    c_decimal decimal(20,3)
) DUPLICATE KEY(id)
  DISTRIBUTED BY HASH(id)
  BUCKETS 1
  PROPERTIES('replication_num'='1');
insert into t_reverse_types values
    (1, 123,  1234567890123, true,  '2024-01-15', '2024-01-15 12:34:56', 3.14,  123.456),
    (2, -456, -9876543210,   false, '1999-12-31', '1999-12-31 23:59:59', -2.71, -99.990),
    (3, 0,    0,             NULL,  NULL,          NULL,                  NULL,  NULL);
drop table t_reverse_types;