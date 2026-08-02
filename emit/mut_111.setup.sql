CREATE TABLE array_data_type
    (c1 int,
    c2  array<bigint>, 
    c3  array<bigint>,
    c4  array<bigint> not null, 
    c5  array<bigint> not null)
    PRIMARY KEY(c1) 
    DISTRIBUTED BY HASH(c1) 
    BUCKETS 1 
    PROPERTIES ("replication_num" = "1");
insert into array_data_type (c1, c2, c3, c4,c5) values 
    (1,NULL,NULL,[22, 11, 33],[22, 11, 33]);
insert into array_data_type (c1, c2, c3, c4,c5) values 
    (2,NULL,[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    (3,[22, 11, 33],[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    (4,[22, 11, 33],NULL,[22, 11, 33],[22, 11, 33]);
insert into array_data_type (c1, c2, c3, c4,c5) values 
    (5,[22, 11, 33],[22, 11, 33],[22, 11, 44],[22, 11, 33]);
CREATE TABLE array_data_type_1
    (c1 int,
    c2  array<datetime>,
    c3  array<float>,
    c4  array<varchar(10)>,
    c5  array<varchar(20)>,
    c6  array<array<varchar(10)>>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into array_data_type_1 values
(1, ['2020-11-11', '2021-11-11', '2022-01-01'], [1.23, 1.35, 2.7894], ['a', 'b'], ['sss', 'eee', 'fff'], [['a', 'b']]),
(2, ['2020-01-11', null, '2022-11-01'], [2.23, 2.35, 3.7894], ['aa', null], ['ssss', 'eeee', null], [['a', null], null]),
(3, null, null, null, null, null);
CREATE TABLE array_top_n
    (c1 int,
    c2 array<int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into array_top_n values
(1, [1]),
(2, [5, 6]),
(3, [2, 3, 4]),
(4, [12, 13, 14, 15]),
(5, [7, 8, 9, 10, 11]);
CREATE TABLE array_exprr
    (
    c1 int not null,
    c2 int not null
    )
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");
insert into array_exprr SELECT generate_series, generate_series FROM TABLE(generate_series(1,  13336));
CREATE TABLE `t2` (
  `pk` bigint(20) NOT NULL COMMENT "",
  `aas_1` array<array<array<varchar(65533)>>> NULL COMMENT "",
  `aad_1` array<array<array<DECIMAL128(26,2)>>> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);
insert into t2 values
(1, [[["10"],["20"],["30"]],[["60"],["5"],["4"]],[["-100","-2"],["-20","10"],["100","23"]]], [[[1.00],[2.00],[3.00]],[[6.00],[5.00],[4.00]],[[-1.00,-2.00],[-2.00,10.00],[100.00,23.00]]]);
CREATE TABLE IF NOT EXISTS repeat_test (COLA INT, COLB INT) PROPERTIES ("replication_num"="1");
INSERT INTO repeat_test (COLA, COLB) VALUES (1, 3), (NULL, 3), (2, NULL);
CREATE TABLE IF NOT EXISTS flatten_test (COLA INT, COLB ARRAY<ARRAY<INT>>) PROPERTIES ("replication_num"="1");
INSERT INTO flatten_test (COLA, COLB) VALUES (1, [[1, 2], [1, 4]]), (2, NULL), (3, [[5], [6, 7, 8], [9]]), (4, [[2, 3], [4, 5, 6], NULL]);
CREATE TABLE IF NOT EXISTS flatten_one_layer_arr_test (COLA INT, COLB ARRAY<INT>) PROPERTIES ("replication_num"="1");
INSERT INTO flatten_one_layer_arr_test (COLA, COLB) VALUES (1, [1, 2, 3]);
CREATE TABLE IF NOT EXISTS null_or_empty_test (COLA INT, COLB ARRAY<INT>) PROPERTIES ("replication_num"="1");
INSERT INTO null_or_empty_test (COLA, COLB) VALUES (1, []), (2, NULL), (3, [1,2]);