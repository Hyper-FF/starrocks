create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));
create table t1 (
    c0 STRING NOT NULL,
    c1 STRING,
    c2 int,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
create table t2 (
    c0 int NOT NULL,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
create table t3 (
    c0 int,
    c1 int NOT NULL,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t1 select * from t0;
insert into t2 select * from t0;
insert into t3 select * from t0;
create table t4 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t4 SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  100000));
insert into t4 SELECT generate_series % 4, null, null, null FROM TABLE(generate_series(1,  100000));
create table t5 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t5 select * from t4;
create table t6 (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t6 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));
create table tempty (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
create table tarray (
    c0 int,
    c1 array<int>,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into tarray SELECT generate_series, [generate_series], generate_series FROM TABLE(generate_series(1,  100000));