CREATE TABLE json_test_data (
  id int,
  target_json json,
  candidate_json json
);
INSERT INTO json_test_data VALUES
(1, parse_json('{"name": "John", "age": 30, "city": "New York"}'), parse_json('{"name": "John"}')),
(2, parse_json('{"products": ["apple", "banana", "orange"]}'), parse_json('["apple", "banana"]')),
(3, parse_json('[1, 2, 3, 4, 5]'), parse_json('3')),
(4, parse_json('{"users": [{"id": 1, "active": true}, {"id": 2, "active": false}]}'), parse_json('{"users": [{"id": 1}]}')),
(5, parse_json('null'), parse_json('null'));
DROP TABLE json_test_data;