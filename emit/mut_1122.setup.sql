CREATE TABLE t1_basic (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t1_basic VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(1, 1));
CREATE TABLE t2_nulls (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t2_nulls VALUES
(1, row(1, 1)),
(2, row(1, NULL)),
(3, row(NULL, 1)),
(4, row(NULL, NULL)),
(5, NULL);
CREATE TABLE t3_mixed_types (
    id INT,
    s STRUCT<name STRING, age INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t3_mixed_types VALUES
(1, row('Alice', 30)),
(2, row('Alice', 25)),
(3, row('Bob', 30)),
(4, row('Bob', 25)),
(5, row('Alice', 30));
CREATE TABLE t3_float (
    id INT,
    s STRUCT<x DOUBLE, y DOUBLE>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t3_float VALUES
(1, row(1.1, 2.2)),
(2, row(1.1, 2.1)),
(3, row(1.2, 2.2)),
(4, row(1.0, 2.0));
CREATE TABLE t3_date (
    id INT,
    s STRUCT<event_date DATE, event_time DATETIME>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t3_date VALUES
(1, row('2024-01-01', '2024-01-01 10:00:00')),
(2, row('2024-01-01', '2024-01-01 09:00:00')),
(3, row('2024-01-02', '2024-01-02 10:00:00')),
(4, row('2024-01-01', '2024-01-01 10:00:00'));
CREATE TABLE t4_nested (
    id INT,
    s STRUCT<outer1 INT, inner1 STRUCT<a INT, b INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t4_nested VALUES
(1, row(1, row(1, 1))),
(2, row(1, row(1, 2))),
(3, row(1, row(2, 1))),
(4, row(2, row(1, 1)));
CREATE TABLE t5_mixed_columns (
    id INT,
    s STRUCT<a INT, b INT>,
    score INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t5_mixed_columns VALUES
(1, row(1, 1), 100),
(2, row(1, 2), 90),
(3, row(2, 1), 100),
(4, row(1, 1), 95);
CREATE TABLE t6_single_field (
    id INT,
    s STRUCT<value INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t6_single_field VALUES
(1, row(3)),
(2, row(1)),
(3, row(2)),
(4, row(1));
CREATE TABLE t7_many_fields (
    id INT,
    s STRUCT<f1 INT, f2 INT, f3 INT, f4 INT, f5 INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t7_many_fields VALUES
(1, row(1, 1, 1, 1, 1)),
(2, row(1, 1, 1, 1, 2)),
(3, row(1, 1, 1, 2, 1)),
(4, row(1, 1, 2, 1, 1)),
(5, row(1, 2, 1, 1, 1)),
(6, row(2, 1, 1, 1, 1));
CREATE TABLE t8_stability (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t8_stability VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 1)),
(4, row(2, 2)),
(5, row(2, 2));
CREATE TABLE t9_limit (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t9_limit VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1)),
(6, row(3, 2));
CREATE TABLE t10_aggregate (
    id INT,
    s STRUCT<a INT, b INT>,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t10_aggregate VALUES
(1, row(1, 1), 10),
(2, row(1, 1), 20),
(3, row(1, 2), 30),
(4, row(2, 1), 40);
CREATE TABLE t11_left (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
CREATE TABLE t11_right (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t11_left VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1));
INSERT INTO t11_right VALUES
(1, row(1, 1)),
(2, row(1, 3)),
(3, row(2, 1));
CREATE TABLE t12_union1 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
CREATE TABLE t12_union2 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t12_union1 VALUES
(1, row(1, 1)),
(2, row(2, 1));
INSERT INTO t12_union2 VALUES
(3, row(1, 2)),
(4, row(2, 2));
CREATE TABLE t13_subquery (
    id INT,
    s STRUCT<a INT, b INT>,
    category STRING
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t13_subquery VALUES
(1, row(1, 1), 'A'),
(2, row(1, 2), 'A'),
(3, row(2, 1), 'B'),
(4, row(2, 2), 'B');
CREATE TABLE t14_cte (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t14_cte VALUES
(1, row(2, 1)),
(2, row(1, 2)),
(3, row(1, 1)),
(4, row(2, 2));
CREATE TABLE t15_extreme (
    id INT,
    s STRUCT<a BIGINT, b BIGINT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t15_extreme VALUES
(1, row(9223372036854775807, 1)),  
(2, row(-9223372036854775808, 1)), 
(3, row(0, 0)),
(4, row(1, -9223372036854775808));
CREATE TABLE t16_string (
    id INT,
    s STRUCT<str1 STRING, str2 STRING>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t16_string VALUES
(1, row('abc', 'xyz')),
(2, row('ABC', 'xyz')),
(3, row('abc', 'XYZ')),
(4, row('Abc', 'xyz'));
CREATE TABLE t17_distinct (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t17_distinct VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 2)),
(4, row(1, 2)),
(5, row(2, 1));
CREATE TABLE t18_multi_struct (
    id INT,
    s1 STRUCT<a INT, b INT>,
    s2 STRUCT<x INT, y INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t18_multi_struct VALUES
(1, row(1, 1), row(1, 1)),
(2, row(1, 1), row(1, 2)),
(3, row(1, 2), row(1, 1)),
(4, row(1, 1), row(2, 1));
CREATE TABLE t19_performance (
    id INT,
    s STRUCT<a INT, b INT, c INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
INSERT INTO t19_performance
SELECT 
    number,
    row(
        number % 100,
        number % 50,
        number % 25
    )
FROM TABLE(generate_series(1, 10000));
CREATE TABLE t20_where (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO t20_where VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1));