CREATE TABLE json_test_table (
  id INT,
  json_data JSON
) DUPLICATE KEY(id);
INSERT INTO json_test_table VALUES
(1, parse_json('{"name": "Alice", "age": 30, "city": "New York"}')),
(2, parse_json('{"name": "Bob", "age": 25, "city": "San Francisco", "hobbies": ["reading", "gaming"]}')),
(3, parse_json('{"product": "laptop", "price": 999.99, "specs": {"cpu": "Intel", "ram": "16GB"}}')),
(4, parse_json('{"empty": {}, "array": [1, 2, 3, 4, 5]}')),
(5, parse_json('null'));
DROP TABLE json_test_table;