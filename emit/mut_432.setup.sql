create table t0 (
  i32 int not null,
  i64 bigint not null,
  d32 decimal(9,2) not null,
  s   varchar(20) not null,
  dt  date not null,
  ts  datetime not null
) PROPERTIES ("replication_num"="1");
insert into t0 values (5, 5, 10.65, 'abc', '2024-01-01', '2024-01-01 12:00:00');
drop table t0;