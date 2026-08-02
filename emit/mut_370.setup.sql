create table t_dup (k1 int, v1 string)
    duplicate key(k1)
    distributed by hash(k1) buckets 16
    properties('replication_num' = '1', 'file_bundling' = 'true');
insert into t_dup select generate_series, concat('val_', generate_series) from TABLE(generate_series(1, 1000));
insert into t_dup select generate_series, concat('val_', generate_series) from TABLE(generate_series(1001, 2000));
insert into t_dup select generate_series, concat('val_', generate_series) from TABLE(generate_series(2001, 3000));
create table t_pk (k1 int, v1 string)
    primary key(k1)
    distributed by hash(k1) buckets 16
    properties('replication_num' = '1', 'file_bundling' = 'true');
insert into t_pk select generate_series, concat('val_', generate_series) from TABLE(generate_series(1, 1000));
insert into t_pk select generate_series, concat('val_', generate_series) from TABLE(generate_series(1001, 2000));
insert into t_pk select generate_series, concat('val_', generate_series) from TABLE(generate_series(2001, 3000));
insert into t_pk select generate_series, concat('updated_', generate_series) from TABLE(generate_series(500, 600));
insert into t_pk select generate_series, concat('new_', generate_series) from TABLE(generate_series(3001, 3500));
create table t_rollback (k1 int, v1 string)
    duplicate key(k1)
    distributed by hash(k1) buckets 16
    properties('replication_num' = '1', 'file_bundling' = 'true');
insert into t_rollback select generate_series, concat('original_', generate_series) from TABLE(generate_series(1, 500));
insert into t_rollback select generate_series, concat('should_disappear_', generate_series) from TABLE(generate_series(501, 1000));
insert into t_rollback select generate_series, concat('also_disappear_', generate_series) from TABLE(generate_series(1001, 1500));
create table t_nobundle (k1 int, v1 string)
    duplicate key(k1)
    distributed by hash(k1) buckets 16
    properties('replication_num' = '1', 'file_bundling' = 'false');
insert into t_nobundle select generate_series, concat('val_', generate_series) from TABLE(generate_series(1, 1000));
insert into t_nobundle select generate_series, concat('val_', generate_series) from TABLE(generate_series(1001, 2000));