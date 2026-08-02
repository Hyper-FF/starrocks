create table t_required (
  id bigint not null,
  arr array<int>,
  opt_id bigint
) properties ("format-version" = "2");
insert into t_required values (1, [10, 20], 100), (2, [30, 40], 200), (3, [50], 300);
drop table t_required force;
create table t_required (
  id bigint not null,
  arr array<int>,
  opt_id bigint
) duplicate key(id) distributed by hash(id) buckets 1 properties ("replication_num" = "1");
insert into t_required values (1, [10, 20], 100), (2, [30, 40], 200), (3, [50], 300);