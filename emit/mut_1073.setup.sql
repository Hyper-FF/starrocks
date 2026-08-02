CREATE TABLE js2 (
    v1 BIGINT NULL,
    j1 JSON NULL
)
DUPLICATE KEY (v1)
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES ( "replication_num" = "1" );
CREATE TABLE js3 (
    v1 BIGINT NULL,
    j1 JSON NULL
)
DUPLICATE KEY (v1)
DISTRIBUTED BY HASH(`v1`) BUCKETS 1
PROPERTIES ( "replication_num" = "1" );
create view profile_late_materialize as
select trim(unnest) as line
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%LateMaterializeRows%'
and trim(unnest) not like '- %: 0'
order by unnest;
create view profile_filter_rows as
select trim(unnest) as line
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%FilterRows%'
and trim(unnest) not like '- %: 0'
order by unnest;
create view profile_dict as
select trim(unnest) as line
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%DictDecodeCount%'
and trim(unnest) not like '- %: 0'
order by unnest;
create view profile_access_path as
select trim(unnest) as line
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%AccessPath%'
and trim(unnest) not like '- %: 0'
order by unnest;
insert into js2 
select 
    generate_series, 
    json_object('f1', generate_series)
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object('f2', generate_series)
from (table(generate_series(1, 1000)));
insert into js3
select 
    generate_series, 
    json_object('f1', concat('a', generate_series%10))
from (table(generate_series(1, 1000)));
insert into js2 
select 
    generate_series, 
    json_object('f1', generate_series, 'f2', generate_series)
from (table(generate_series(1, 1000)));
insert into js3
select generate_series, json_object(
        'f_bool', cast(generate_series % 2 = 0 as boolean),
        'f_int', generate_series,
        'f_int1', generate_series,
        'f_int2', generate_series,
        'f_double', cast(generate_series as double) * 1.0
        )
from (table(generate_series(1, 1000)));