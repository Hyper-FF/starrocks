CREATE TABLE json_set_test_table (
    id INT, 
    data JSON
) DUPLICATE KEY(id)
PROPERTIES("replication_num" = "1");
INSERT INTO json_set_test_table VALUES 
(1, parse_json('{"name": "Alice", "tags": ["A"]}')),
(2, parse_json('{"name": "Bob", "score": 10}')),
(3, NULL),
(4, parse_json('{}'));
DROP TABLE json_set_test_table;