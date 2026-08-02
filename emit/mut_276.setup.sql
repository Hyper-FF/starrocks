CREATE TABLE test_json_strict_valid (
    id INT,
    data1 JSON DEFAULT '{"key": "value"}',
    data2 JSON DEFAULT '{"name": "Alice", "age": 30}',
    data3 JSON DEFAULT '{}',
    data4 JSON DEFAULT 'null',
    data5 JSON DEFAULT '""',
    data6 JSON DEFAULT '123',
    data7 JSON DEFAULT 'true'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_json_strict_valid (id) VALUES (1);
INSERT INTO test_json_strict_valid_complex (id) VALUES (1);
INSERT INTO test_json_strict_valid_nested (id) VALUES (1);
INSERT INTO test_json_strict_valid_array (id) VALUES (1);
INSERT INTO test_json_strict_edge_cases (id) VALUES (1);