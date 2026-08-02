create table array_struct_nest(c1 int, c2 array<struct<c2_sub1 int, c2_sub2 int>>)
  duplicate key(c1) distributed by hash(c1) buckets 1 properties("replication_num"="1");
insert into array_struct_nest values
  (1, [row(5,100),row(6,200)]),
  (2, [row(5,300),row(7,400)]),
  (3, [row(8,500)]);