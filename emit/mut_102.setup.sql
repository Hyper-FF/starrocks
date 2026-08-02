create table t1(c1 int, c2 bigint, c3 string, c4 string)
properties('replication_num'='1');
insert into t1 values (1, 1, 's1', 's1');
insert into t1 values (2, 2, 's2', 's2');
insert into t1 values (3, 3, 's3', 's3');
insert into t1 select generate_series, generate_series, generate_series, generate_series from table(generate_series(1,1000));
insert into t1 select generate_series, generate_series, generate_series, generate_series from table(generate_series(1,1000));
insert into t1 select generate_series, generate_series, generate_series, generate_series from table(generate_series(1,1000));
insert into t1 select generate_series, generate_series, generate_series, generate_series from table(generate_series(1,1000));
insert into t1 select generate_series, generate_series, generate_series, generate_series from table(generate_series(1,1000));