CREATE TABLE IF NOT EXISTS test_struct_topn (
    id INT,
    info STRUCT<category STRING, value INT, timestamp DATETIME>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO test_struct_topn VALUES
    (1, named_struct('category', 'A', 'value', 100, 'timestamp', '2024-01-01 10:00:00')),
    (2, named_struct('category', 'B', 'value', 200, 'timestamp', '2024-01-02 10:00:00')),
    (3, named_struct('category', 'A', 'value', 150, 'timestamp', '2024-01-03 10:00:00')),
    (4, named_struct('category', 'C', 'value', 180, 'timestamp', '2024-01-04 10:00:00')),
    (5, named_struct('category', 'B', 'value', 120, 'timestamp', '2024-01-05 10:00:00')),
    (6, named_struct('category', 'A', 'value', 100, 'timestamp', '2024-01-06 10:00:00')),
    (7, named_struct('category', 'C', 'value', 220, 'timestamp', '2024-01-07 10:00:00')),
    (8, named_struct('category', 'B', 'value', 190, 'timestamp', '2024-01-08 10:00:00')),
    (9, named_struct('category', 'A', 'value', 110, 'timestamp', '2024-01-09 10:00:00')),
    (10, named_struct('category', 'C', 'value', 160, 'timestamp', '2024-01-10 10:00:00'));
DROP TABLE test_struct_topn FORCE;