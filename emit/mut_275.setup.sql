CREATE TABLE basic_json_types (
    id INT NOT NULL,
    json_object JSON DEFAULT '{"status": "active", "count": 0}',
    json_array JSON DEFAULT '[1, 2, 3]',
    json_string JSON DEFAULT '"hello"',
    json_number JSON DEFAULT '42',
    json_boolean JSON DEFAULT 'true',
    json_null JSON DEFAULT 'null',
    empty_object JSON DEFAULT '{}',
    empty_array JSON DEFAULT '[]',
    empty_string JSON DEFAULT '',
    no_default JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO basic_json_types (id) VALUES (1);
CREATE TABLE without_default (
    id INT,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO with_default (id) VALUES (1);
INSERT INTO without_default (id) VALUES (1);
INSERT INTO empty_string_test (id) VALUES (1);
INSERT INTO empty_string_test VALUES (2, '', '""');
INSERT INTO fast_schema_change VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE fast_schema_change ADD COLUMN metadata JSON DEFAULT '{"version": 1, "enabled": true}';
INSERT INTO traditional_schema_change VALUES (1, 100), (2, 200);
ALTER TABLE traditional_schema_change ADD COLUMN metadata JSON DEFAULT '{"source": "migration", "timestamp": 0}';
INSERT INTO extended_column_basic VALUES 
    (1, '{"user": {"name": "alice", "age": 25}}'),
    (2, '{"user": {"name": "bob", "age": 30}}');
ALTER TABLE extended_column_basic ADD COLUMN profile JSON DEFAULT '{"level": 1, "vip": false, "tags": ["default"]}';
INSERT INTO extended_default_inherit_types SELECT 1;
ALTER TABLE extended_default_inherit_types
ADD COLUMN json_col JSON DEFAULT '{
  "i_str": "222",
  "i_num": 223,
  "b_str": "true",
  "b_bool": true,
  "d_str": "1.25",
  "d_num": 2.5,
  "s": "hello",
  "nullv": null,
  "obj": {"x": "7"},
  "arr": ["9"]
}';
INSERT INTO extended_default_inherit_types
SELECT 3, '{"i_str":"333","i_num":334,"b_str":"false","b_bool":false,"d_str":"3.75","d_num":4.5,"s":"world","nullv":null,"obj":{"x":"8"},"arr":["10"]}';
INSERT INTO extended_complex_types VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE extended_complex_types 
ADD COLUMN profile JSON DEFAULT '{"level": 10, "vip": true, "score": 95.5, "tags": ["gold", "premium"], "meta": {"city": "Beijing", "age": 25}}';
INSERT INTO extended_multi_json VALUES (1, 'user1'), (2, 'user2');
ALTER TABLE extended_multi_json ADD COLUMN config JSON DEFAULT '{"theme": "dark", "lang": "en"}';
INSERT INTO extended_null_values VALUES (1), (2);
ALTER TABLE extended_null_values ADD COLUMN data JSON DEFAULT '{"value": null, "count": 0, "name": "test"}';
INSERT INTO extended_empty_structures VALUES (1), (2);
ALTER TABLE extended_empty_structures ADD COLUMN info JSON DEFAULT '{"tags": [], "meta": {}}';
INSERT INTO extended_deep_nested VALUES (1), (2);
ALTER TABLE extended_deep_nested ADD COLUMN deep JSON DEFAULT '{"level1": {"level2": {"level3": {"value": 42, "flag": true}}}}';
INSERT INTO extended_arrays VALUES (1), (2), (3);
ALTER TABLE extended_arrays ADD COLUMN items JSON DEFAULT '{"products": ["apple", "banana", "orange"], "prices": [1.5, 2.0, 1.8]}';
INSERT INTO extended_function_compat VALUES (1), (2);
ALTER TABLE extended_function_compat ADD COLUMN profile JSON DEFAULT '{"user": {"name": "Alice", "age": 25}, "score": 95}';
INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (2, 'phone');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (3, 'tablet');
INSERT INTO pk_partial_update (order_id, product_name, metadata) 
VALUES (5, 'keyboard', '{"source": "mobile", "version": 2}');
INSERT INTO pk_partial_update (order_id, product_name, quantity) VALUES (7, 'headset', 3);
CREATE TABLE flatjson_field_absence (
    id INT NOT NULL,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO flatjson_field_absence VALUES 
    (1, '{"name": "alice", "age": 25, "city": "Beijing"}'),
    (2, '{"name": "bob", "age": 30, "city": "Shanghai"}'),
    (3, '{"name": "charlie", "age": 35, "city": "Guangzhou"}');
INSERT INTO flatjson_field_absence VALUES 
    (4, '{"name": "david", "age": 40, "city": "Shenzhen", "optional_field": "value1"}'),
    (5, '{"name": "eve", "age": 45, "city": "Hangzhou", "optional_field": "value2"}');