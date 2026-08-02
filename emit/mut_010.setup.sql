create table t_lake (k1 int, v1 string) primary key(k1)
    distributed by hash(k1) buckets 1
    properties('replication_num' = '1', 'file_bundling' = 'true', 'enable_persistent_index' = 'true');
insert into t_lake values (1, 'a');