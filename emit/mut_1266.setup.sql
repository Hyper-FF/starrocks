create table ts_arr(id int, arr array<struct<c1 int, c2 int>>)
  duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1");
insert into ts_arr values (1, [row(10,1),row(20,2)]), (2, [row(30,3)]);