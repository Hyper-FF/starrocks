CREATE TABLE IF NOT EXISTS test_array_struct (
    id INT,
    tags ARRAY<STRUCT<tag_key STRING, tag_value INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
INSERT INTO test_array_struct VALUES
    (1, [row('a', 1), row('b', 2)]),
    (2, [row('a', 1), row('b', 1)]),
    (3, [row('a', 2), row('b', 1)]),
    (4, [row('a', 1)]),
    (5, []);
INSERT INTO test_array_struct_lengths VALUES
    (1, [row('Alice', 90)]),
    (2, [row('Alice', 90), row('Bob', 80)]),
    (3, [row('Alice', 90), row('Bob', 80), row('Charlie', 85)]),
    (4, []),
    (5, [row('Alice', 85)]);
INSERT INTO test_array_struct_complex VALUES
    (1, [row('login', 1000, row('web', 1))]),
    (2, [row('login', 1000, row('mobile', 2))]),
    (3, [row('login', 999, row('web', 1))]),
    (4, [row('logout', 1000, row('web', 1))]);
INSERT INTO test_array_struct_join VALUES
    (1, [row('tech', 1)]),
    (2, [row('sports', 2)]),
    (3, [row('tech', 2)]);
INSERT INTO test_array_struct_compare VALUES
    (1, [row(1, 2), row(3, 4)]),
    (2, [row(1, 2), row(3, 3)]),
    (3, [row(1, 2), row(2, 4)]),
    (4, [row(1, 2)]);
DROP TABLE test_array_struct_lengths FORCE;
DROP TABLE test_array_struct_complex FORCE;
DROP TABLE test_array_struct_join FORCE;
DROP TABLE test_array_struct_compare FORCE;