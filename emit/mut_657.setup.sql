CREATE TABLE map_test (
pk bigint not null ,
map0  map<Int,string>,
map1  map<DECIMAL(16, 3),varchar(30)>,
map2  map<int, array<string>>,
map3  map<string, map<int, varchar(30)>>,
map4  map<int, json>,
map5  map<INT, STRUCT<c INT, b string>>
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into map_test values (0, map(0,'ab'),map(0,'ab'),map(0,['1','2']),map('1',map(1,'abc')),map(1,json_object('name','abc','age',23)),map(0, row(1,'a')));
insert into map_test values (1, map(1, null),map(null,''),map(1,[]),map('11',map(1,'abc'),'', map(2,null)),map(null,json_object('name',null)),map(null, row(null,null)));
insert into map_test values (2, map(null,null),map(null,null),map(null,null),map(null,map(null,null)),map(null,null),map(null, row(null,null)));
insert into map_test values (3, map(3,'',null,null),map(3,'',null,null),map(3,['3',null], null,null),map('3',map(3,'a33c'),null,null),map(null,null,1,json_object('name','abc','age',23)),map(null,null,3, row(3,'a')));
insert into map_test values (4, null,null,null,null,null,null);