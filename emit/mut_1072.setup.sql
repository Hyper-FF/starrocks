create view profile_decode as
select trim(unnest) 
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%DICT_DECODE%'
order by unnest;
CREATE TABLE js2 (
    v1 BIGINT NULL,
    c1 JSON NULL
)
DUPLICATE KEY (v1)
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES ( "replication_num" = "1" );
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', concat('a', generate_series % 10),
        'f2', concat('a', generate_series % 100),
        'f3', concat('a', generate_series % 200),
        'f4', concat('a', generate_series % 500)
        )
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', generate_series % 10,                                 
        'f2', cast((generate_series % 20) * 1.5 as double),         
        'f3', if(generate_series % 2 = 0, true, false),             
        'f4', null,                                                 
        'f5', json_array(generate_series % 5, 'arr', 1.23)          
        )
from (table(generate_series(1, 100)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', concat('a', generate_series % 10),
        'f2', concat('a', generate_series % 100),
        'f3', concat('a', generate_series % 200),
        'f4', concat('a', generate_series % 500)
        )
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', concat('a', generate_series % 10),
        'f2', concat('a', generate_series % 300),
        'f3', concat('a', generate_series % 500),
        'f4', concat('a', generate_series % 500)
        )
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', concat('a', generate_series % 1000),
        'f2', concat('a', generate_series % 1000),
        'f3', concat('a', generate_series % 1000),
        'f4', concat('a', generate_series % 1000)
        )
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', concat('a', generate_series % 10),
        'f2', concat('a', generate_series % 20),
        'f3', concat('a', generate_series % 30),
        'f4', concat('a', generate_series % 40),
        'f5', concat('a', generate_series % 50)
        )
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object(
        'f1', generate_series % 10,                                 
        'f2', cast((generate_series % 20) * 1.5 as double),         
        'f3', if(generate_series % 2 = 0, true, false),             
        'f4', null,                                                 
        'f5', json_array(generate_series % 5, 'arr', 1.23)          
        )
from (table(generate_series(1, 1000)));
CREATE TABLE js3 (
    v1 BIGINT NULL,
    c1 JSON NULL
)
DUPLICATE KEY (v1)
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES ( "replication_num" = "1" );
insert into js3
select 
    generate_series, 
    json_object(
        'str_field', concat('str_', generate_series % 10),
        'int_field', generate_series % 100,
        'float_field', (generate_series % 100) * 1.23,
        'bool_field', if(generate_series % 2 = 0, true, false),
        'null_field', null,
        'array_field', json_array(
            generate_series % 5, 
            concat('arr_', generate_series % 3), 
            (generate_series % 10) * 0.5
        ),
        'object_field', json_object(
            'nested_str', concat('nested_', generate_series % 7),
            'nested_int', generate_series % 7,
            'nested_bool', if(generate_series % 3 = 0, true, false)
        ),
        'deep_nested', json_object(
            'level1', json_object(
                'level2', json_array(
                    json_object(
                        'leaf_str', concat('leaf_', generate_series % 2),
                        'leaf_num', generate_series % 2
                    ),
                    generate_series % 2
                )
            )
        )
    )
from (table(generate_series(1, 1000)));