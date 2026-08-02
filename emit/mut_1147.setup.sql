create table t1(c1 int, c2 int) properties("replication_num"="1");
create table t2(c1 int, c2 int) properties("replication_num"="1");
insert into t1 select generate_series,generate_series from table(generate_series(0,2000));
insert into t2 select generate_series,generate_series from table(generate_series(0,2000));