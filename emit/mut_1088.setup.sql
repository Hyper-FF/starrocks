create table `t1` (
  `c0` string not null,
  `c1` string not null,
  `c2` datetime null,
  `data` json null,
  `id` bigint  not null auto_increment
) duplicate key(`c0`)
distributed by hash(`c0`) buckets 1
properties('replication_num'='1');
create table `t2` (
  `c0` string not null,
  `c1` string not null,
  `c2` datetime null,
  `data` json null,
  `id` bigint  not null auto_increment
) duplicate key(`c0`)
distributed by hash(`c0`) buckets 1
properties('replication_num'='1');
insert into `t1` (`c0`,`c1`,`c2`,`data`) select @uuid_val, @uuid_val, '2020-01-01 00:00:00', @json_val from table(generate_series(1, 10000, 1));
insert into `t2` select * from t1;