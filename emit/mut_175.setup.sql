create table t_struct_binary (
    id      int,
    payload struct<bin_field varbinary, str_field varchar(64)>
) duplicate key(id)
distributed by hash(id)
buckets 1
properties('replication_num'='1');
insert into t_struct_binary
select 1, named_struct('bin_field', unhex('DEADBEEF'), 'str_field', 'hello')
union all
select 2, named_struct('bin_field', unhex('CAFE'),     'str_field', 'world')
union all
select 3, named_struct('bin_field', null,              'str_field', 'null_bin');
create table t_array_binary (
    id   int,
    bins array<varbinary>
) duplicate key(id)
distributed by hash(id)
buckets 1
properties('replication_num'='1');
insert into t_array_binary values
    (1, [unhex('AABB'), unhex('CCDD')]),
    (2, [unhex('00'), null]),
    (3, null);
create table t_map_binary (
    id int,
    kv map<varchar(32), varbinary>
) duplicate key(id)
distributed by hash(id)
buckets 1
properties('replication_num'='1');
insert into t_map_binary values
    (1, map{'k1': unhex('CAFE'), 'k2': unhex('BABE')}),
    (2, map{'only': null});