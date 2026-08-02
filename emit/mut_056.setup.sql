CREATE TABLE tbinary_ndv_test (
    id INT,
    data VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO tbinary_ndv_test
SELECT generate_series,
       to_binary(CONCAT('value_', CAST(generate_series AS VARCHAR)), 'utf8'),
       (generate_series - 1) % 5 + 1
FROM TABLE(GENERATE_SERIES(1, 100));
CREATE TABLE tbinary_ndv_null_test (
    id INT,
    data VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO tbinary_ndv_null_test VALUES
(1, to_binary('aaa', 'utf8'), 1),
(2, to_binary('bbb', 'utf8'), 1),
(3, NULL, 1),
(4, to_binary('aaa', 'utf8'), 2),
(5, to_binary('ccc', 'utf8'), 2),
(6, NULL, 2),
(7, to_binary('ddd', 'utf8'), 3),
(8, to_binary('eee', 'utf8'), 3),
(9, to_binary('fff', 'utf8'), 3);
CREATE TABLE tbinary_ndv_dup_test (
    id INT,
    data VARBINARY
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO tbinary_ndv_dup_test VALUES
(1, to_binary('same_value', 'utf8')),
(2, to_binary('same_value', 'utf8')),
(3, to_binary('same_value', 'utf8')),
(4, to_binary('different_value', 'utf8')),
(5, to_binary('different_value', 'utf8')),
(6, NULL),
(7, NULL);
CREATE TABLE tbinary_multi_distinct_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    data3 VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO tbinary_multi_distinct_test VALUES
(1, to_binary('value1', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
(2, to_binary('value2', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
(3, to_binary('value1', 'utf8'), to_binary('val2', 'utf8'), to_binary('v2', 'utf8'), 1),
(4, to_binary('value3', 'utf8'), to_binary('val2', 'utf8'), to_binary('v1', 'utf8'), 2),
(5, to_binary('value4', 'utf8'), to_binary('val3', 'utf8'), to_binary('v3', 'utf8'), 2),
(6, to_binary('value3', 'utf8'), to_binary('val3', 'utf8'), to_binary('v2', 'utf8'), 2),
(7, to_binary('value5', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
(8, to_binary('value6', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
(9, to_binary('value5', 'utf8'), to_binary('val5', 'utf8'), to_binary('v5', 'utf8'), 3);
CREATE TABLE tbinary_multi_null_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO tbinary_multi_null_test VALUES
(1, to_binary('val1', 'utf8'), to_binary('valA', 'utf8'), 1),
(2, NULL, to_binary('valA', 'utf8'), 1),
(3, to_binary('val1', 'utf8'), NULL, 1),
(4, NULL, NULL, 1),
(5, to_binary('val2', 'utf8'), to_binary('valB', 'utf8'), 2),
(6, to_binary('val2', 'utf8'), NULL, 2),
(7, NULL, to_binary('valB', 'utf8'), 2);
DROP TABLE IF EXISTS tbinary_ndv_test;
DROP TABLE IF EXISTS tbinary_ndv_null_test;
DROP TABLE IF EXISTS tbinary_ndv_dup_test;
DROP TABLE IF EXISTS tbinary_multi_distinct_test;
DROP TABLE IF EXISTS tbinary_multi_null_test;