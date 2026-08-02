Create table t ( int0 int not null, int1 int not null, nint0 int , nint1 int) ENGINE=OLAP DUPLICATE KEY(int0)  DISTRIBUTED BY HASH(int0) BUCKETS 4 PROPERTIES("replication_num" = "1");
insert into t values (0,0,0,0),(1,1,1,1),(2,2,null,null),(3,3,30,null),(4,4,null,40),(0,1,null,null),(0,0,null,1),(10,10,null,10);
create table dict(col_1 int, col_2 string, col_3 bigint not null auto_increment, col_4 int)
                primary key(col_1)
                distributed by hash(col_1)
PROPERTIES (
"replication_num" = "1"
);
insert into dict values(1, 'hello world 1', default, 1 * 10);
insert into dict values(2, 'hello world 2', default, 2 * 10);
insert into dict values(3, 'hello world 3', default, 3 * 10);
insert into dict values(4, 'hello world 4', default, 4 * 10);