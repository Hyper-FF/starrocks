create table t1(k1 int);
insert into t1 select sleep(4);
insert into t1 select sleep(4);
insert into t1 properties("timeout" = "10") select sleep(4);
create table t2 as select sleep(4) as k1;
create table t2 as select sleep(4) as k1;