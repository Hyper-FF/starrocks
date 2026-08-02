CREATE TABLE json_pretty_test_table (
    id INT,
    data JSON
) DUPLICATE KEY(id)
PROPERTIES("replication_num" = "1");
INSERT INTO json_pretty_test_table VALUES
(1, parse_json('{"name": "Alice", "details": {"age": 25}}')),
(2, parse_json('[10, 20]')),
(3, NULL);
DROP TABLE json_pretty_test_table;