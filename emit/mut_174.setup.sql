create table t0(c0 INT, c1 varbinary(16), c2 varchar(16)) 
        DUPLICATE KEY(c0) 
        DISTRIBUTED BY HASH(c0) 
        BUCKETS 1 
        PROPERTIES('replication_num'='1');
insert into t0 values (1, x'ab01', 'ab01');
insert into t0 values (2, x'abab', 'abab');