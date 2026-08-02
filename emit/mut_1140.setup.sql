create table unique_tbl(a int, b string)
unique key(a)
distributed by hash(a)
PROPERTIES('replication_num' = '1');
insert into unique_tbl values(1,'hi'),(2,'hello');