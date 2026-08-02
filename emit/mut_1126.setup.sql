CREATE TABLE IF NOT EXISTS test_struct_edge (
    id INT,
    category STRING,
    info STRUCT<name STRING, score INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO test_struct_edge VALUES
    (1, 'A', named_struct('name', 'Alice', 'score', 90)),
    (2, 'A', named_struct('name', 'Bob', 'score', 85)),
    (3, 'B', named_struct('name', 'Charlie', 'score', 92)),
    (4, 'B', named_struct('name', 'David', 'score', 88));
DROP TABLE test_struct_edge FORCE;