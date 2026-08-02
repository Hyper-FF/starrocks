create table t1(k int, v int not null);
create table t2(k int, v int not null) primary key(k) distributed by hash(k);
insert into t2 select * from t1;