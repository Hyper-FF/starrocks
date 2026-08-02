CREATE TABLE array_primitives (id INT) DUPLICATE KEY(id) 
DISTRIBUTED BY HASH(id) BUCKETS 1 
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO array_primitives VALUES (1), (2);
ALTER TABLE array_primitives ADD COLUMN arr_tinyint ARRAY<TINYINT> DEFAULT [1, 2, 127];
ALTER TABLE array_primitives ADD COLUMN arr_smallint ARRAY<SMALLINT> DEFAULT [100, 200, 32767];
ALTER TABLE array_primitives ADD COLUMN arr_int ARRAY<INT> DEFAULT [1000, 2000, 2147483647];
ALTER TABLE array_primitives ADD COLUMN arr_bigint ARRAY<BIGINT> DEFAULT [1000000, 2000000, 9223372036854775807];
ALTER TABLE array_primitives ADD COLUMN arr_float ARRAY<FLOAT> DEFAULT [1.1, 2.2, 3.3];
ALTER TABLE array_primitives ADD COLUMN arr_double ARRAY<DOUBLE> DEFAULT [1.123456789, 2.987654321, 3.141592653];
ALTER TABLE array_primitives ADD COLUMN arr_varchar ARRAY<VARCHAR(50)> DEFAULT ['hello', 'world', '你好世界'];
ALTER TABLE array_primitives ADD COLUMN arr_char ARRAY<CHAR(10)> DEFAULT ['abc', 'def', 'ghi'];
ALTER TABLE array_primitives ADD COLUMN arr_bool ARRAY<BOOLEAN> DEFAULT [true, false, true, false];
ALTER TABLE array_primitives ADD COLUMN arr_decimal32 ARRAY<DECIMAL(9, 2)> DEFAULT [123.45, 678.90, 999.99];
ALTER TABLE array_primitives ADD COLUMN arr_decimal64 ARRAY<DECIMAL(18, 4)> DEFAULT [12345678.1234, 87654321.4321];
ALTER TABLE array_primitives ADD COLUMN arr_decimal128 ARRAY<DECIMAL(38, 10)> DEFAULT [1234567890.1234567890, 9876543210.0987654321];
ALTER TABLE array_primitives ADD COLUMN arr_decimal256 ARRAY<DECIMAL(55, 10)> DEFAULT [1123123234567890123412345678901234.1234567890, 98765432109876.0987654321];
CREATE TABLE map_types (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO map_types VALUES (1), (2);
ALTER TABLE map_types ADD COLUMN map_int_str MAP<INT, VARCHAR(20)> DEFAULT map{1: 'one', 2: 'two', 100: 'hundred'};
ALTER TABLE map_types ADD COLUMN map_str_int MAP<VARCHAR(20), INT> DEFAULT map{'alice': 25, 'bob': 30, 'charlie': 35};
ALTER TABLE map_types ADD COLUMN map_int_double MAP<INT, DOUBLE> DEFAULT map{1: 1.1, 2: 2.2, 3: 3.3};
ALTER TABLE map_types ADD COLUMN map_str_bool MAP<VARCHAR(20), BOOLEAN> DEFAULT map{'active': true, 'disabled': false};
ALTER TABLE map_types ADD COLUMN map_int_decimal MAP<INT, DECIMAL(10, 2)> DEFAULT map{1: 99.99, 2: 199.99, 3: 299.99};
CREATE TABLE struct_field_order (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO struct_field_order VALUES (1), (2);
ALTER TABLE struct_field_order ADD COLUMN st1 STRUCT<z INT, a VARCHAR(20)> DEFAULT row(999, 'test');
ALTER TABLE struct_field_order ADD COLUMN st2 STRUCT<field_c INT, field_b VARCHAR(20), field_a DOUBLE> 
DEFAULT row(100, 'middle', 3.14);
ALTER TABLE struct_field_order ADD COLUMN st3 STRUCT<s4 INT, ks ARRAY<INT>> DEFAULT row(2, [1, 2, 3, 4]);
ALTER TABLE struct_field_order ADD COLUMN st4 STRUCT<
    zebra INT,
    apple VARCHAR(20),
    monkey BOOLEAN,
    banana DOUBLE
> DEFAULT row(10, 'fruit', true, 2.5);
CREATE TABLE nested_arrays (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO nested_arrays VALUES (1), (2);
ALTER TABLE nested_arrays ADD COLUMN arr2_int ARRAY<ARRAY<INT>> DEFAULT [[1, 2, 3], [4, 5], [6, 7, 8, 9]];
ALTER TABLE nested_arrays ADD COLUMN arr2_str ARRAY<ARRAY<VARCHAR(20)>> DEFAULT [['a', 'b'], ['c', 'd', 'e'], ['f']];
ALTER TABLE nested_arrays ADD COLUMN arr3_int ARRAY<ARRAY<ARRAY<INT>>> DEFAULT [[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]];
ALTER TABLE nested_arrays ADD COLUMN arr2_empty ARRAY<ARRAY<INT>> DEFAULT [[], [1, 2], []];
CREATE TABLE array_struct (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO array_struct VALUES (1), (2);
ALTER TABLE array_struct ADD COLUMN arr_st1 ARRAY<STRUCT<id INT, name VARCHAR(20)>> 
DEFAULT [row(1, 'alice'), row(2, 'bob'), row(3, 'charlie')];
ALTER TABLE array_struct ADD COLUMN arr_st2 ARRAY<STRUCT<score INT, age INT, name VARCHAR(20)>> 
DEFAULT [row(95, 25, 'student1'), row(88, 26, 'student2')];
ALTER TABLE array_struct ADD COLUMN arr_st3 ARRAY<STRUCT<id INT, tags ARRAY<VARCHAR(20)>>> 
DEFAULT [row(1, ['tag1', 'tag2']), row(2, ['tag3', 'tag4', 'tag5'])];
CREATE TABLE map_array (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO map_array VALUES (1), (2);
ALTER TABLE map_array ADD COLUMN mp_arr1 MAP<INT, ARRAY<VARCHAR(20)>> 
DEFAULT map{1: ['a', 'b', 'c'], 2: ['d', 'e'], 3: ['f', 'g', 'h', 'i']};
ALTER TABLE map_array ADD COLUMN mp_arr2 MAP<VARCHAR(20), ARRAY<INT>> 
DEFAULT map{'scores': [90, 85, 92], 'ages': [25, 30, 35]};
ALTER TABLE map_array ADD COLUMN mp_arr3 MAP<INT, ARRAY<ARRAY<INT>>> 
DEFAULT map{1: [[1, 2], [3, 4]], 2: [[5], [6, 7, 8]]};
CREATE TABLE map_struct (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO map_struct VALUES (1), (2);
ALTER TABLE map_struct ADD COLUMN mp_st1 MAP<INT, STRUCT<name VARCHAR(20), age INT>> 
DEFAULT map{1: row('alice', 25), 2: row('bob', 30)};
ALTER TABLE map_struct ADD COLUMN mp_st2 MAP<INT, STRUCT<z INT, a VARCHAR(20)>> 
DEFAULT map{10: row(999, 'test'), 20: row(888, 'demo')};
ALTER TABLE map_struct ADD COLUMN mp_st3 MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>>> 
DEFAULT map{1: row(2, [1, 2, 3, 4]), 2: row(5, [10, 20, 30])};
ALTER TABLE map_struct ADD COLUMN mp_st4 MAP<INT, STRUCT<
    field_b VARCHAR(20),
    field_a INT,
    nested STRUCT<z INT, a VARCHAR(20)>
>> DEFAULT map{10: row('hello', 100, row(999, 'world'))};
CREATE TABLE struct_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO struct_nested VALUES (1), (2);
ALTER TABLE struct_nested ADD COLUMN st_arr STRUCT<id INT, scores ARRAY<INT>, name VARCHAR(20)> 
DEFAULT row(1, [90, 85, 95], 'student');
ALTER TABLE struct_nested ADD COLUMN st_map STRUCT<id INT, attributes MAP<VARCHAR(20), VARCHAR(20)>> 
DEFAULT row(1, map{'color': 'red', 'size': 'large'});
ALTER TABLE struct_nested ADD COLUMN st_both STRUCT<
    tags ARRAY<VARCHAR(20)>,
    id INT,
    metadata MAP<VARCHAR(20), INT>
> DEFAULT row(['tag1', 'tag2'], 100, map{'version': 1, 'priority': 5});
ALTER TABLE struct_nested ADD COLUMN st_deep STRUCT<
    name VARCHAR(20),
    items ARRAY<STRUCT<id INT, value VARCHAR(20)>>
> DEFAULT row('container', [row(1, 'item1'), row(2, 'item2')]);
CREATE TABLE three_level_nesting (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO three_level_nesting VALUES (1), (2);
ALTER TABLE three_level_nesting ADD COLUMN arr3 ARRAY<ARRAY<ARRAY<INT>>> 
DEFAULT [[[1, 2], [3]], [[4, 5]]];
ALTER TABLE three_level_nesting ADD COLUMN arr_map_arr ARRAY<MAP<INT, ARRAY<INT>>> 
DEFAULT [map{1: [1, 2], 2: [3, 4]}, map{10: [10, 20]}];
ALTER TABLE three_level_nesting ADD COLUMN map_arr_st MAP<INT, ARRAY<STRUCT<id INT, name VARCHAR(20)>>> 
DEFAULT map{1: [row(1, 'a'), row(2, 'b')]};
ALTER TABLE three_level_nesting ADD COLUMN st_map_arr STRUCT<
    id INT,
    data MAP<VARCHAR(20), ARRAY<INT>>
> DEFAULT row(1, map{'scores': [90, 95, 100]});
ALTER TABLE three_level_nesting ADD COLUMN map_st_arr MAP<INT, STRUCT<
    name VARCHAR(20),
    vals ARRAY<INT>
>> DEFAULT map{1: row('test', [1, 2, 3])};
CREATE TABLE empty_collections (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO empty_collections VALUES (1), (2);
ALTER TABLE empty_collections ADD COLUMN arr_empty ARRAY<INT> DEFAULT [];
ALTER TABLE empty_collections ADD COLUMN map_empty MAP<INT, VARCHAR(20)> DEFAULT map{};
ALTER TABLE empty_collections ADD COLUMN st_empty_arr STRUCT<id INT, tags ARRAY<VARCHAR(20)>> 
DEFAULT row(1, []);
ALTER TABLE empty_collections ADD COLUMN st_empty_map STRUCT<id INT, attrs MAP<VARCHAR(20), INT>> 
DEFAULT row(1, map{});
ALTER TABLE empty_collections ADD COLUMN st_all_empty STRUCT<
    id INT,
    arr ARRAY<INT>,
    mp MAP<VARCHAR(20), INT>
> DEFAULT row(0, [], map{});
ALTER TABLE empty_collections ADD COLUMN arr_mixed ARRAY<ARRAY<INT>> 
DEFAULT [[], [1, 2], [], [3, 4, 5]];
ALTER TABLE empty_collections ADD COLUMN map_empty_arr MAP<INT, ARRAY<VARCHAR(20)>> 
DEFAULT map{1: [], 2: ['a', 'b'], 3: []};
CREATE TABLE decimal_complex (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO decimal_complex VALUES (1), (2);
ALTER TABLE decimal_complex ADD COLUMN arr_dec32 ARRAY<DECIMAL(9, 2)> DEFAULT [99.99, 199.99, 299.99];
ALTER TABLE decimal_complex ADD COLUMN arr_dec64 ARRAY<DECIMAL(18, 4)> DEFAULT [9999.9999, 19999.9999];
ALTER TABLE decimal_complex ADD COLUMN arr_dec128 ARRAY<DECIMAL(38, 10)> DEFAULT [123456.1234567890, 654321.0987654321];
ALTER TABLE decimal_complex ADD COLUMN arr_dec256 ARRAY<DECIMAL(45, 10)> DEFAULT [123456789012.1234567890, 987654321098.0987654321];
ALTER TABLE decimal_complex ADD COLUMN map_dec MAP<VARCHAR(20), DECIMAL(10, 2)> 
DEFAULT map{'price': 99.99, 'tax': 8.25, 'total': 108.24};
ALTER TABLE decimal_complex ADD COLUMN map_dec256 MAP<INT, DECIMAL(45, 10)> 
DEFAULT map{1: 99999999999.9999999999, 2: 12345678901.1234567890};
ALTER TABLE decimal_complex ADD COLUMN st_dec STRUCT<
    amount DECIMAL(10, 2),
    rate DECIMAL(5, 4),
    result DECIMAL(15, 6),
    large DECIMAL(45, 10)
> DEFAULT row(1000.50, 0.0525, 52.526250, 123456789012.1234567890);
ALTER TABLE decimal_complex ADD COLUMN arr_st_dec ARRAY<STRUCT<id INT, price DECIMAL(10, 2)>> 
DEFAULT [row(1, 99.99), row(2, 199.99), row(3, 299.99)];
CREATE TABLE large_defaults (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO large_defaults VALUES (1);
ALTER TABLE large_defaults ADD COLUMN large_arr ARRAY<INT> 
DEFAULT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
ALTER TABLE large_defaults ADD COLUMN large_map MAP<INT, VARCHAR(20)> 
DEFAULT map{1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 
            6: 'six', 7: 'seven', 8: 'eight', 9: 'nine', 10: 'ten'};
ALTER TABLE large_defaults ADD COLUMN large_struct STRUCT<
    f1 INT, f2 INT, f3 INT, f4 INT, f5 INT,
    f6 VARCHAR(20), f7 VARCHAR(20), f8 DOUBLE, f9 BOOLEAN, f10 DECIMAL(10, 2)
> DEFAULT row(1, 2, 3, 4, 5, 'six', 'seven', 8.8, true, 10.50);
ALTER TABLE large_defaults ADD COLUMN complex_combo MAP<INT, STRUCT<
    id INT,
    tags ARRAY<VARCHAR(20)>,
    scores ARRAY<INT>,
    metadata MAP<VARCHAR(20), VARCHAR(20)>
>> DEFAULT map{
    1: row(100, ['tag1', 'tag2', 'tag3'], [90, 85, 95], map{'type': 'A', 'level': 'high'}),
    2: row(200, ['tag4', 'tag5'], [80, 88], map{'type': 'B', 'level': 'medium'})
};
CREATE TABLE special_strings (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO special_strings VALUES (1);
ALTER TABLE special_strings ADD COLUMN arr_unicode ARRAY<VARCHAR(50)> 
DEFAULT ['你好', '世界', 'こんにちは', '안녕하세요', '🚀', '⭐'];
ALTER TABLE special_strings ADD COLUMN arr_special ARRAY<VARCHAR(50)> 
DEFAULT ['it\'s', 'path\\to\\file', '"quoted"'];
ALTER TABLE special_strings ADD COLUMN map_unicode MAP<VARCHAR(20), VARCHAR(20)> 
DEFAULT map{'中文': '测试', 'emoji': '😀🎉'};
ALTER TABLE special_strings ADD COLUMN st_unicode STRUCT<name VARCHAR(50), description VARCHAR(100)> 
DEFAULT row('用户名', '这是一个包含中文的描述信息');
CREATE TABLE deep_nesting_1 (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO deep_nesting_1 VALUES (1);
ALTER TABLE deep_nesting_1 ADD COLUMN arr_st_map_arr ARRAY<STRUCT<
    id INT,
    data MAP<INT, ARRAY<INT>>
>> DEFAULT [
    row(1, map{10: [1, 2, 3], 20: [4, 5]}),
    row(2, map{30: [6, 7, 8, 9]})
];
ALTER TABLE deep_nesting_1 ADD COLUMN map_st_arr_map MAP<INT, STRUCT<
    tags ARRAY<VARCHAR(20)>,
    attrs MAP<VARCHAR(20), INT>
>> DEFAULT map{
    1: row(['tag1', 'tag2'], map{'score': 90, 'level': 5}),
    2: row(['tag3'], map{'score': 85, 'level': 3})
};
ALTER TABLE deep_nesting_1 ADD COLUMN st_map_st_arr STRUCT<
    name VARCHAR(20),
    data MAP<INT, STRUCT<id INT, vals ARRAY<INT>>>
> DEFAULT row('test', map{1: row(100, [1, 2, 3]), 2: row(200, [4, 5])});
CREATE TABLE deep_nesting_2 (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO deep_nesting_2 VALUES (1);
ALTER TABLE deep_nesting_2 ADD COLUMN st_map_arr_map STRUCT<
    id INT,
    nested MAP<INT, ARRAY<MAP<VARCHAR(20), INT>>>
> DEFAULT row(1, map{
    10: [map{'a': 1, 'b': 2}, map{'c': 3}],
    20: [map{'d': 4, 'e': 5, 'f': 6}]
});
ALTER TABLE deep_nesting_2 ADD COLUMN arr_map_st_map_arr ARRAY<MAP<INT, STRUCT<
    name VARCHAR(20),
    data MAP<VARCHAR(20), ARRAY<INT>>
>>> DEFAULT [
    map{1: row('item1', map{'scores': [90, 85], 'ages': [25, 30]})},
    map{2: row('item2', map{'scores': [95, 92, 88]})}
];
ALTER TABLE deep_nesting_2 ADD COLUMN map_arr_st_arr_st MAP<INT, ARRAY<STRUCT<
    id INT,
    items ARRAY<STRUCT<k VARCHAR(20), v INT>>
>>> DEFAULT map{
    1: [row(10, [row('a', 1), row('b', 2)])],
    2: [row(20, [row('c', 3)]), row(30, [row('d', 4), row('e', 5)])]
};
CREATE TABLE complex_field_order (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO complex_field_order VALUES (1);
ALTER TABLE complex_field_order ADD COLUMN arr_st_order ARRAY<STRUCT<
    zebra INT,
    apple VARCHAR(20),
    monkey BOOLEAN,
    banana ARRAY<INT>
>> DEFAULT [
    row(10, 'fruit', true, [1, 2, 3]),
    row(20, 'animal', false, [4, 5])
];
ALTER TABLE complex_field_order ADD COLUMN map_st_nested MAP<INT, STRUCT<
    outer_z INT,
    outer_a VARCHAR(20),
    nested_st STRUCT<inner_y INT, inner_b VARCHAR(20)>
>> DEFAULT map{
    1: row(100, 'test', row(200, 'nested'))
};
ALTER TABLE complex_field_order ADD COLUMN st_map_st STRUCT<
    id INT,
    data MAP<INT, STRUCT<s4 INT, ks ARRAY<INT>, aa VARCHAR(20)>>
> DEFAULT row(1, map{
    10: row(2, [1, 2, 3, 4], 'test'),
    20: row(5, [10, 20], 'demo')
});
CREATE TABLE mixed_empty_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO mixed_empty_nested VALUES (1);
ALTER TABLE mixed_empty_nested ADD COLUMN arr3_mixed ARRAY<ARRAY<ARRAY<INT>>> 
DEFAULT [[[1, 2], []], [[]], [[3], [4, 5]]];
ALTER TABLE mixed_empty_nested ADD COLUMN map_arr_st_empty MAP<INT, ARRAY<STRUCT<
    id INT,
    tags ARRAY<VARCHAR(20)>
>>> DEFAULT map{
    1: [row(1, []), row(2, ['tag1', 'tag2'])],
    2: []
};
ALTER TABLE mixed_empty_nested ADD COLUMN st_mixed_empty STRUCT<
    id INT,
    empty_arr ARRAY<INT>,
    empty_map MAP<VARCHAR(20), INT>,
    non_empty_arr ARRAY<INT>,
    non_empty_map MAP<VARCHAR(20), INT>
> DEFAULT row(1, [], map{}, [1, 2], map{'k': 100});
ALTER TABLE mixed_empty_nested ADD COLUMN arr_map_st_empty ARRAY<MAP<INT, STRUCT<
    id INT,
    data ARRAY<INT>
>>> DEFAULT [
    map{},
    map{1: row(10, [1, 2, 3])},
    map{},
    map{2: row(20, [])}
];
CREATE TABLE six_level_nesting (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO six_level_nesting VALUES (1);
ALTER TABLE six_level_nesting ADD COLUMN level6_1 ARRAY<MAP<INT, STRUCT<
    id INT,
    nested ARRAY<MAP<INT, ARRAY<INT>>>
>>> DEFAULT [
    map{1: row(100, [map{1: [1, 2]}, map{2: [3, 4]}])}
];
ALTER TABLE six_level_nesting ADD COLUMN level6_2 MAP<INT, STRUCT<
    name VARCHAR(20),
    data MAP<INT, ARRAY<STRUCT<id INT, vals ARRAY<INT>>>>
>> DEFAULT map{
    1: row('test', map{10: [row(1, [1, 2, 3])]})
};
ALTER TABLE six_level_nesting ADD COLUMN level6_3 STRUCT<
    id INT,
    deep ARRAY<MAP<INT, STRUCT<
        name VARCHAR(20),
        data MAP<INT, ARRAY<INT>>
    >>>
> DEFAULT row(1, [
    map{1: row('item', map{10: [1, 2], 20: [3, 4]})}
]);
CREATE TABLE decimal256_nested (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO decimal256_nested VALUES (1);
ALTER TABLE decimal256_nested ADD COLUMN arr_st_dec256 ARRAY<STRUCT<
    id INT,
    amount DECIMAL(45, 10)
>> DEFAULT [
    row(1, 12345678901234.1234567890),
    row(2, 98765432109876.0987654321)
];
ALTER TABLE decimal256_nested ADD COLUMN map_st_dec256 MAP<INT, STRUCT<
    price DECIMAL(45, 10),
    history ARRAY<DECIMAL(45, 10)>
>> DEFAULT map{
    1: row(99999999999.9999999999, [11111111111.1111111111, 22222222222.2222222222])
};
ALTER TABLE decimal256_nested ADD COLUMN st_map_arr_dec256 STRUCT<
    name VARCHAR(20),
    data MAP<INT, ARRAY<STRUCT<id INT, val DECIMAL(45, 10)>>>
> DEFAULT row('test', map{
    1: [row(10, 12345.1234567890), row(20, 67890.0987654321)]
});
CREATE TABLE t_ns (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
INSERT INTO t_ns VALUES (1), (2);
ALTER TABLE t_ns
ADD COLUMN st_outer STRUCT<
  k1 INT,
  mid STRUCT<
    a INT,
    sub STRUCT<x INT, y VARCHAR(20)>
  >,
  k2 VARCHAR(20)
>
DEFAULT row(10, row(1, row(7, 'yy')), 'end');
CREATE TABLE t_prune_mix (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
INSERT INTO t_prune_mix VALUES (1), (2);
ALTER TABLE t_prune_mix
ADD COLUMN st_mix STRUCT<
  sid INT,
  arr ARRAY<STRUCT<s INT, meta STRUCT<x INT, y VARCHAR(10)>>>,
  mp MAP<INT, STRUCT<m VARCHAR(10), vals ARRAY<INT>>>,
  name VARCHAR(10)
>
DEFAULT row(
  1,
  [row(10, row(7, 'a')), row(20, row(8, 'b'))],
  map{1: row('v', [1, 2, 3]), 2: row('w', [9])},
  'n'
);
CREATE TABLE t_prune_arr_mp (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
INSERT INTO t_prune_arr_mp VALUES (1), (2);
ALTER TABLE t_prune_arr_mp
ADD COLUMN arr_mp ARRAY<MAP<INT, STRUCT<a INT, b STRUCT<x INT, y VARCHAR(10)>>>>
DEFAULT [
  map{1: row(10, row(7, 'aa')), 2: row(20, row(8, 'bb'))}
];
CREATE TABLE t_prune_mp_arr (id INT) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1", "fast_schema_evolution"="true");
INSERT INTO t_prune_mp_arr VALUES (1), (2);
ALTER TABLE t_prune_mp_arr
ADD COLUMN mp_arr MAP<VARCHAR(10), ARRAY<STRUCT<id INT, meta STRUCT<p INT, q VARCHAR(10)>>>>
DEFAULT map{
  'k': [row(1, row(7, 'qq')), row(2, row(8, 'ww'))]
};