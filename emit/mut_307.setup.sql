create table t0 (
v1 int,
v2 int,
v3 int
) properties("replication_num"="1");
insert into t0 select i%13 as v1, i%17 as v2, i % 37 as v3 from table(generate_series(1,100000)) t(i);