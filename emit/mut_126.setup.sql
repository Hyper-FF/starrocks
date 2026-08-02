CREATE TABLE t1 (
    id INT(11) not null,
    array_col1 ARRAY<INT>,
    array_col2 ARRAY<DOUBLE>,
    array_col3 ARRAY<VARCHAR(20)>,
    array_col4 ARRAY<DATE>
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES
(1, [4, 3, 5], [1.1, 2.2, 2.2], ['a', 'b', 'c'], ['2023-01-01', '2023-01-02', '2023-01-03']),
(2, [6, 7, 8], [6.6, 5.5, 6.6], ['d', 'e', 'd'], ['2023-01-04', '2023-01-05', '2023-01-06']),
(3, NULL, [7.7, 8.8, 8.8], ['g', 'h', 'h'], ['2023-01-07', '2023-01-08', '2023-01-09']),
(4, [9, 10, 11], NULL, ['k', 'k', 'j'], ['2023-01-10', '2023-01-12', '2023-01-11']),
(5, [12, 13, 14], [10.10, 11.11, 11.11], NULL, ['2023-01-13', '2023-01-14', '2023-01-15']),
(6, [15, 16, 17], [14.14, 13.13, 14.14], ['m', 'o', 'o'], NULL),
(7, [18, 19, 20], [16.16, 16.16, 18.18], ['p', 'p', 'r'], ['2023-01-16', NULL, '2023-01-18']),
(8, [21, 22, 23], [19.19, 20.20, 19.19], ['a', 't', 'a'], ['2023-01-19', '2023-01-20', '2023-01-21']),
(9, [24, 25, 26], NULL, ['y', 'y', 'z'], ['2023-01-25', '2023-01-24', '2023-01-26']),
(10, [24, 25, 26], NULL, ['y', 'y', 'z'], ['2023-01-25', NULL, '2023-01-26']);
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
CREATE TABLE __row_util (
  idx bigint NULL,
  array_c1 ARRAY<INT>
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util 
select 
    row_number() over() as idx,
    array_generate(10)
from __row_util_base;
CREATE TABLE t1 (
    id INT(11) not null,
    int_1 ARRAY<INT>,
    int_2 ARRAY<INT>,
    str_1 ARRAY<VARCHAR(20)>,
    date_1 ARRAY<DATE>
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 
select
    idx,
    array_c1,
    array_map(array_c1, x -> case when idx % 13 != 0 then x % 3 else null end),
    array_map(array_c1, x -> case when idx % 13 != 0 then concat('abc-', x % 5) else null end),
    array_map(array_c1, x -> case when idx % 13 != 0 then date_sub('2023-11-02', interval cast(x % 2 as int) day) else null end)
from __row_util;
CREATE TABLE t2 (
    id INT(11) not null,
    int_1 ARRAY<INT>,
    int_2 ARRAY<INT>,
    str_1 ARRAY<VARCHAR(20)>,
    date_1 ARRAY<DATE>
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into t2 
select
    idx,
    array_c1,
    case when idx % 11 != 0 then array_map(array_c1, x -> case when idx % 13 != 0 then x % 3 else null end) else null end,
    case when idx % 11 != 0 then array_map(array_c1, x -> case when idx % 13 != 0 then concat('abc-', x % 5) else null end) else null end,
    case when idx % 11 != 0 then array_map(array_c1, x -> case when idx % 13 != 0 then date_sub('2023-11-02', interval cast(x % 2 as int) day) else null end) else null end
from __row_util;
CREATE TABLE t3 (
    id INT(11) not null,
    int_1 ARRAY<INT> not null,
    int_2 ARRAY<INT> not null,
    str_1 ARRAY<VARCHAR(20)> not null,
    date_1 ARRAY<DATE> not null
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(id) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into t3 
select
    idx,
    array_c1,
    array_map(array_c1, x -> x % 3),
    array_map(array_c1, x -> concat('abc-', x % 5)),
    array_map(array_c1, x -> date_sub('2023-11-02', interval cast(x % 2 as int) day))
from __row_util;
CREATE TABLE test_array_sortby (
    id INT,
    array_boolean ARRAY<BOOLEAN>,
    array_tinyint ARRAY<TINYINT>,
    array_smallint ARRAY<SMALLINT>,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_largeint ARRAY<LARGEINT>,
    array_float ARRAY<FLOAT>,
    array_double ARRAY<DOUBLE>,
    array_decimal32 ARRAY<DECIMAL32(9, 2)>,
    array_decimal64 ARRAY<DECIMAL64(18, 2)>,
    array_decimal128 ARRAY<DECIMAL128(38, 10)>,
    array_decimalv2 ARRAY<DECIMALV2>,
    array_varchar ARRAY<VARCHAR(100)>,
    array_datetime ARRAY<DATETIME>,
    array_date ARRAY<DATE>,
    array_json ARRAY<JSON>
) 
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
INSERT INTO test_array_sortby VALUES
(1, [true, false], [1, 3], [100, 50], [10, 5], [1000, 500], [9223372036854775806, 9223372036854775807], 
 [1.5, 2.5], [2.345, 1.234], [12345.67, 45678.90], [1234567890.12, 987654321.12], 
 [12345678901234567890.1234567890, 9876543210987654321.1234567890], 
 [12345.67, 67890.12], ['apple', 'banana'], 
 ['2025-01-24 10:30:00', '2024-12-31 23:59:59'], 
 ['2025-01-24', '2024-12-31'], 
 ['{"key":"value1"}', '{"key":"value2"}']),
(2, [false, true], [5, 7], [200, 150], [30, 20], [1500, 1250], [9223372036854775803, 9223372036854775802],
 [0.1, 0.3], [5.678, 4.567], [56789.01, 67890.12], [2345678901.23, 3456789012.34],
 [456789123456789123.1234567890, 123456789012345678.9876543210],
 [34567.89, 23456.78], ['grape', 'pear'],
 ['2025-02-01 12:00:00', '2025-01-01 08:15:00'],
 ['2025-02-01', '2025-01-15'],
 ['{"key":"value4"}', '{"key":"value5"}']),
(3, [true, true], [2, 1], [75, 100], [15, 10], [750, 1000], [9223372036854775807, 9223372036854775806],
 [1.5, 0.5], [3.456, 1.234], [78901.12, 12345.67], [987654321.12, 1234567890.12],
 [9876543210987654321.1234567890, 12345678901234567890.1234567890],
 [78901.34, 12345.67], ['cherry', 'apple'],
 ['2025-01-01 00:00:00', '2024-12-31 23:59:59'],
 ['2025-01-01', '2024-12-31'],
 ['{"key":"value7"}', '{"key":"value8"}']),
(4, [false], [6], [300], [45], [2000], [9223372036854775804],
 [0.8], [6.789], [12345.67], [6789012345.67],
 [7891234567890123456.1234567890],
 [45678.90], ['mango'],
 ['2025-03-01 10:10:10'],
 ['2025-03-01'],
 ['{"key":"value10"}']),
(5, [true, false, true], [9, 3, 6], [400, 200, 100], [60, 30, 10], [3000, 1500, 500], [9223372036854775805, 9223372036854775803, 9223372036854775802], 
 [2.0, 1.0, 3.0], [7.123, 6.234, 8.456], [45678.12, 23456.34, 78901.56], [3456789012.45, 2345678901.23, 1234567890.12], 
 [1234567890123456789.1234567890, 987654321098765432.1234567890, 456789123456789123.1234567890], 
 [56789.12, 23456.34, 78901.56], ['kiwi', 'orange', 'pear'], 
 ['2025-04-15 15:30:00', '2025-04-16 10:00:00', '2025-04-17 08:15:00'], 
 ['2025-04-15', '2025-04-16', '2025-04-17'], 
 ['{"key":"value11"}', '{"key":"value12"}', '{"key":"value13"}']),
(6, [false, false], [1, 1], [10, 20], [5, 15], [50, 150], [9223372036854775801, 9223372036854775801], 
 [0.5, 0.5], [1.123, 1.123], [1234.56, 1234.56], [123456.78, 123456.78], 
 [1234567890.1234567890, 1234567890.1234567890], 
 [1234.56, 1234.56], ['lemon', 'lime'], 
 ['2025-05-01 12:00:00', '2025-05-01 12:00:00'], 
 ['2025-05-01', '2025-05-01'], 
 ['{"key":"value14"}', '{"key":"value14"}']),
(7, [true], [8], [250], [20], [1200], [9223372036854775806], 
 [1.7], [4.321], [23456.78], [345678901.23], 
 [9876543210123456789.9876543210], 
 [12345.67], ['melon'], 
 ['2025-06-01 13:30:00'], 
 ['2025-06-01'], 
 ['{"key":"value15"}']),
(8, [false, true, false], [2, 4, 8], [75, 100, 50], [15, 10, 5], [1500, 1000, 500], [9223372036854775807, 9223372036854775806, 9223372036854775805],
 [2.5, 1.5, 0.5], [1.234, 3.456, 2.345], [12345.67, 78901.12, 45678.90], [987654321.12, 123123123.45, 1234567890.12],
 [12345678901234567890.1234567890, 456789123456789123.1234567890, 9876543210987654321.1234567890],
 [67890.12, 78901.34, 12345.67], ['apple', 'banana', 'cherry'],
 ['2025-01-24 10:30:00', '2024-12-31 23:59:59', '2025-01-01 00:00:00'],
 ['2025-01-24', '2024-12-31', '2025-01-01'],
 ['{"key":"value1"}', '{"key":"value2"}', '{"key":"value3"}']);