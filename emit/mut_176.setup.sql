create table t0(c0 INT, c1 binary(16), c2 binary, c3 varbinary, c4 varchar(16)) 
        DUPLICATE KEY(c0) 
        DISTRIBUTED BY HASH(c0) 
        BUCKETS 1 
        PROPERTIES('replication_num'='1');
insert into t0 values (1, x'ab01', x'abab', x'abac','ab01');
insert into t0 values (2, x'ab01', x'abab', x'abac','ab01');
create table t1(c0 INT, c1 binary(16), c2 binary, c3 varbinary, c4 varchar(16)) 
        DUPLICATE KEY(c0) 
        DISTRIBUTED BY HASH(c0) 
        BUCKETS 1 
        PROPERTIES('replication_num'='1');
insert into t1 select * from t0;
create table t2(c1 int, c2 varbinary) duplicate key(c1) properties("replication_num"="1");