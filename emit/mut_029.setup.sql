create table t0(
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);
create table t1 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t1 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096)) union all select null,null;