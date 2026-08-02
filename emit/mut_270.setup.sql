CREATE TABLE fast_schema_evolution (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO fast_schema_evolution (id) VALUES (1), (2), (3);
ALTER TABLE fast_schema_evolution ADD COLUMN arr_col ARRAY<INT> DEFAULT [10, 20, 30];
INSERT INTO nested_complex (id) VALUES (1), (2);
ALTER TABLE nested_complex ADD COLUMN nested_array ARRAY<ARRAY<INT>> DEFAULT [[1, 2], [3, 4, 5]];
INSERT INTO type_cast_defaults (id) VALUES (1), (2);
ALTER TABLE type_cast_defaults ADD COLUMN m_bigint MAP<BIGINT, BIGINT>
  DEFAULT map<smallint, smallint>{1: 2, 3: 4};
ALTER TABLE type_cast_defaults ADD COLUMN m_int MAP<INT, INT>
  DEFAULT map<tinyint, tinyint>{10: 20, 30: 40};
ALTER TABLE type_cast_defaults ADD COLUMN m_int_double MAP<INT, DOUBLE>
  DEFAULT map<int, float>{1: 1.5, 2: 2.5};
ALTER TABLE type_cast_defaults ADD COLUMN m_mixed MAP<BIGINT, DOUBLE>
  DEFAULT map<smallint, float>{1: 1.5, 2: 2.5};
ALTER TABLE type_cast_defaults ADD COLUMN a_bigint ARRAY<BIGINT>
  DEFAULT array<int>[100, 200, 300];
ALTER TABLE type_cast_defaults ADD COLUMN a_double ARRAY<DOUBLE>
  DEFAULT array<float>[1.5, 2.5, 3.5];
INSERT INTO map_struct_order (id) VALUES (1), (2);
ALTER TABLE map_struct_order ADD COLUMN data MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>>> 
DEFAULT map{1: row(2, [1, 2, 3, 4])};
INSERT INTO empty_collections (id) VALUES (1), (2);
ALTER TABLE empty_collections ADD COLUMN empty_arr ARRAY<INT> DEFAULT [];
INSERT INTO all_primitive_types (id) VALUES (1);
ALTER TABLE all_primitive_types ADD COLUMN arr_int ARRAY<INT> DEFAULT [1, 2, 3];
INSERT INTO nullable_complex (id) VALUES (1), (2);
ALTER TABLE nullable_complex ADD COLUMN nullable_arr ARRAY<INT> NULL DEFAULT [100, 200];
INSERT INTO reorder_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE reorder_test ADD COLUMN arr ARRAY<INT> DEFAULT [1, 2, 3];
INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (3, 'tablet');
INSERT INTO pk_column_mode (order_id, product_name) VALUES (5, 'keyboard');
CREATE TABLE pk_row_mode (
    id INT NOT NULL,
    name VARCHAR(50),
    score INT DEFAULT '100',
    level VARCHAR(20) DEFAULT 'bronze',
    tags ARRAY<VARCHAR(20)> DEFAULT ['default', 'new'],
    config MAP<VARCHAR(20), INT> DEFAULT map{'level': 1, 'score': 100},
    details STRUCT<category VARCHAR(20), active BOOLEAN> DEFAULT row('general', true)
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO pk_row_mode VALUES 
    (1, 'item1', 500, 'gold', ['premium'], map{'level': 5}, row('special', false));
INSERT INTO pk_row_mode (id, name) VALUES (3, 'item3');
INSERT INTO pk_nested_complex (id, name) VALUES (1, 'user1');
INSERT INTO pk_nested_complex (id, name) VALUES (2, 'user2');
INSERT INTO pk_nested_complex (id, name) VALUES (3, 'user3');
INSERT INTO pk_empty_collections (id, status) VALUES (1, 'active');
INSERT INTO pk_empty_collections (id, status) VALUES (2, 'inactive');
CREATE TABLE pk_multi_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    arr_int ARRAY<INT> DEFAULT [10, 20, 30],
    arr_str ARRAY<VARCHAR(20)> DEFAULT ['a', 'b', 'c'],
    map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two'},
    map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'x': 100, 'y': 200},
    struct_simple STRUCT<f1 INT, f2 VARCHAR(20)> DEFAULT row(999, 'default'),
    struct_complex STRUCT<id INT, tags ARRAY<VARCHAR(20)>> DEFAULT row(1, ['tag1', 'tag2'])
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO pk_multi_complex (id, name) VALUES (1, 'test1');
INSERT INTO pk_multi_complex (id, name) VALUES (2, 'test2');
CREATE TABLE pk_decimal_complex (
    id INT NOT NULL,
    name VARCHAR(50),
    prices ARRAY<DECIMAL(10, 2)> DEFAULT [99.99, 199.99, 299.99],
    price_map MAP<VARCHAR(20), DECIMAL(10, 2)> DEFAULT map{'min': 10.00, 'max': 1000.00},
    price_info STRUCT<base DECIMAL(10, 2), tax DECIMAL(5, 2)> DEFAULT row(100.00, 8.25)
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO pk_decimal_complex (id, name) VALUES (1, 'product1');
INSERT INTO pk_decimal_complex (id, name) VALUES (2, 'product2');