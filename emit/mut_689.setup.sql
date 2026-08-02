create table t(id int, j json) duplicate key(id)
  distributed by hash(id) buckets 1 properties("replication_num"="1");
insert into t values
 (1, parse_json('{"o":{"inner":10},"a.b":7}')),
 (2, parse_json('{"o":{"inner":20},"a.b":8}'));