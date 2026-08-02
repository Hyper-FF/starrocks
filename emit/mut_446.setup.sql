create table t1 properties("replication_num" = "1") as
select cast(1 as tinyint) as c1
,cast(1 as smallint) as c2
,cast(1 as int) as c3
,cast(1 as bigint) as c4
,cast(1 as largeint) as c5
,cast(1 as decimal(19, 2)) as c6
,cast(1 as double) as c7
,cast(1 as float) as c8
,cast(1 as boolean) as c9
,cast(1 as char) as c10
,cast(1 as string) as c11
,cast(1 as varchar) as c12
,cast('s' as BINARY) as c13
,cast('2023-03-07' as date) as c14
,cast('2023-03-07 11:22:33' as datetime) as c15
,[1, 2, 3] as c16
,get_json_object('{"k1":1, "k2":"v2"}', '$.k1') as c17
,map{1:"apple", 2:"orange", 3:"pear"} as c18
,struct(1, 2, 3, 4) as c19
,parse_json('{"a": 1, "b": true}') as c20;
CREATE TABLE pv_bitmap (
    dt INT(11) NULL COMMENT "",
    page VARCHAR(10) NULL COMMENT "",
    user_id bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(dt, page)
COMMENT "OLAP"
DISTRIBUTED BY HASH(dt)
properties("replication_num" = "1");
insert into pv_bitmap values(1, 'test', to_bitmap(10));
create table test_uv(
    dt date,
    id int,
    uv_set hll hll_union
)
distributed by hash(id)
properties("replication_num" = "1");
insert into test_uv values('2024-01-01', 1, hll_hash(10));