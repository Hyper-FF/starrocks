create table t0 (
    c0 DATE,
    c1 INT,
    c2 BIGINT
) DUPLICATE key (c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 values ('2024-01-01', 1, 1), ('2024-01-02', 2, 2), ('2024-01-03', 3, 3), ('2024-01-04', 4, 4), ('2024-01-05', 5, 5);
create table t1 (
    c0 DATE,
    c1 INT
) DUPLICATE key (c0) 
PARTITION BY RANGE(c0) (
    PARTITION p202401 VALUES [("2024-01-01"), ("2024-02-01")),
    PARTITION p202402 VALUES [("2024-02-01"), ("2024-03-01"))
)
DISTRIBUTED BY HASH(c1) BUCKETS 3 
PROPERTIES('replication_num' = '1');
insert into t1 values ('2024-01-15', 10), ('2024-01-20', 20), ('2024-02-10', 30), ('2024-02-15', 40);
alter table t1 add temporary partition tp202403 VALUES [("2024-03-01"), ("2024-04-01"));
insert into t1 TEMPORARY PARTITION(tp202403) values ('2024-03-10', 50), ('2024-03-15', 60), ('2024-03-20', 70);
create table t2 (
    c0 INT
) DUPLICATE key (c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t2 values (1), (2), (3);
alter table t2 rename column c0 to c1;
alter table t2 rename column c1 to c2;