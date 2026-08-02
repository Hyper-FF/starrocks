create table t0(c0 INT, c1 array<int>)duplicate key(c0) partition by range(`c0`)(PARTITION `p100` VALUES LESS THAN ('100'),PARTITION `p1000` VALUES LESS THAN ('1000'),PARTITION `p10000` VALUES LESS THAN ('10000'))distributed by hash(c0) buckets 1 properties('replication_num'='1');
insert into t0 values(99, [1,2,3]),(999, [4,5,6]),(9999, [7,8,9]);
insert into blackhole() select * from t0;