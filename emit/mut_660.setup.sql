CREATE TABLE struct_test (
pk bigint not null,
s0  struct<c0 int,c1 string>,
s1  struct<c0 DECIMAL(16, 3),c1 varchar(30)>,
s2  struct<c0 int,c1 array<string>>,
s3  struct<c0 string,c1 map<int, varchar(30)>>,
s4  struct<c0 int,c1 json>,
s5  struct<c0 INT,c1 STRUCT<c INT, b string>>
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into struct_test (pk,s0) values (7,row(3,'',null,null));
insert into struct_test values (0, row(0,'ab'),row(0,'ab'),row(0,['1','2']),row('1',map(1,'abc')),row(1,json_object('name','abc','age',23)),row(0, row(1,'a')));
insert into struct_test values (1, row(1, null),row(null,''),row(1,[]),row('11',map(1,'abc','',null)),row(null,json_object('name',null)),row(null, row(null,null)));
insert into struct_test values (2, row(null,null),row(null,null),row(null,null),row(null,map(null,null)),row(null,null),row(null, row(null,null)));
insert into struct_test values (3, row(3,''),row(3,''),row(3,['3',null, null,null]),row('3',map(3,'a33c',null,null)),row(null,json_object('name','abc','age',23)),row(null, row(3,'a')));
insert into struct_test values (4, null,null,null,null,null,null);