create table t_evt(
  transaction_uuid varchar(64) null,
  request_attributes array<struct<name varchar(32), value varchar(32)>> null,
  ts datetime null
) duplicate key(transaction_uuid)
distributed by hash(transaction_uuid) buckets 3 properties("replication_num"="1");
create table t_dim(transaction_uuid varchar(64) null, dim int)
  duplicate key(transaction_uuid) distributed by hash(dim) buckets 3 properties("replication_num"="1");
insert into t_evt
select concat('u', generate_series % 300), [row('n1','v1'), row('n2','v2')], '2026-02-02 03:04:05'
from table(generate_series(1, 3000));
insert into t_dim
select concat('u', generate_series % 300), generate_series from table(generate_series(1, 3000));