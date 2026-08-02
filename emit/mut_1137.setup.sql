create table t0(
    c0 INT,
    c1 BIGINT,
    c2 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t0 values(1, 1, 's1'), (1, 2, 's0'), (1, 1, 's1');
insert into t0 values(2, 2, 's2'), (3, 3, 's3');
insert into t0 values(3, 3, 's3'), (3, 4, 's4'), (3, 3, 's5'), (4, 4, 's6');
insert into t0 select * from t0;
create table t1(
    c0 INT NULL,
    c1 BIGINT NULL,
    c2 STRING NULL
) DUPLICATE KEY(c0, c1) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into t1 select * from t0;
insert into t1 values(null, 1, 's1'), (null, 1, 's2'), (1, null, 's0'), (1, null, 's2'), (null, null, null);
create table t2(
    c0 STRING NULL,
    c1 STRING NULL,
    c2 STRING NULL
) DUPLICATE KEY(c0, c1) DISTRIBUTED BY HASH(c0) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t1 select * from t0;
create table t3(
    c0 INT,
    c1 DECIMAL(10,2),
    c2 DECIMAL(15,3),
    c3 STRING
) DUPLICATE KEY(c0, c1) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t3 values(1, 100.50, 1000.123, 'type1');
insert into t3 values(1, 200.75, 2000.456, 'type2');
insert into t3 values(1, 100.50, 1000.123, 'type1');
insert into t3 values(2, 300.25, 3000.789, 'type3');
insert into t3 values(2, 400.00, 4000.000, 'type4');
insert into t3 values(3, 500.99, 5000.999, 'type5');
insert into t3 values(3, 500.99, 5000.999, 'type5');
create table t4(
    c0 INT,
    c1 DATETIME,
    c2 DATE,
    c3 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t4 values(1, '2023-01-01 10:00:00', '2023-01-01', 'batch1');
insert into t4 values(1, '2023-01-01 10:00:00', '2023-01-01', 'batch1');
insert into t4 values(1, '2023-01-02 11:00:00', '2023-01-02', 'batch2');
insert into t4 values(2, '2023-01-03 12:00:00', '2023-01-03', 'batch3');
insert into t4 values(2, '2023-01-03 12:00:00', '2023-01-03', 'batch3');
insert into t4 values(3, '2023-01-04 13:00:00', '2023-01-04', 'batch4');
create table t5(
    c0 INT,
    c1 BOOLEAN,
    c2 BOOLEAN,
    c3 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t5 values(1, true, false, 'status1');
insert into t5 values(1, true, true, 'status2');
insert into t5 values(1, false, false, 'status3');
insert into t5 values(2, true, false, 'status4');
insert into t5 values(2, false, true, 'status5');
insert into t5 values(3, true, true, 'status6');
create table t6(
    c0 INT,
    c1 FLOAT,
    c2 DOUBLE,
    c3 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t6 values(1, 1.5, 1.123456789, 'float1');
insert into t6 values(1, 2.5, 2.234567890, 'float2');
insert into t6 values(1, 1.5, 1.123456789, 'float1');
insert into t6 values(2, 3.5, 3.345678901, 'float3');
insert into t6 values(2, 4.5, 4.456789012, 'float4');
insert into t6 values(3, 5.5, 5.567890123, 'float5');
create table t7(
    c0 INT,
    c1 LARGEINT,
    c2 STRING
) DUPLICATE KEY(c0, c1) DISTRIBUTED BY HASH(c0) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t7 values(1, 9223372036854775807, 'large1');
insert into t7 values(1, 9223372036854775806, 'large2');
insert into t7 values(1, 9223372036854775807, 'large1');
insert into t7 values(2, 9223372036854775805, 'large3');
insert into t7 values(2, 9223372036854775804, 'large4');
insert into t7 values(3, 9223372036854775803, 'large5');
create table t8(
    c0 INT,
    c1 VARCHAR(50),
    c2 VARCHAR(100),
    c3 STRING
) DUPLICATE KEY(c0, c1) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t8 values(1, 'varchar1', 'longer_varchar1', 'string1');
insert into t8 values(1, 'varchar2', 'longer_varchar2', 'string2');
insert into t8 values(1, 'varchar1', 'longer_varchar1', 'string1');
insert into t8 values(2, 'varchar3', 'longer_varchar3', 'string3');
insert into t8 values(2, 'varchar4', 'longer_varchar4', 'string4');
insert into t8 values(3, 'varchar5', 'longer_varchar5', 'string5');
create table t9(
    c1 STRING,
    c0 INT,
    c2 DECIMAL(10,2),
    c3 DATETIME,
    c4 BOOLEAN
) DUPLICATE KEY(c1, c0) DISTRIBUTED BY HASH(c1) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into t9 values('group1', 1, 100.50, '2023-01-01 10:00:00', true);
insert into t9 values('group1', 1, 100.50, '2023-01-01 10:00:00', true);
insert into t9 values('group1', 1, 200.75, '2023-01-01 11:00:00', false);
insert into t9 values('group2', 1, 300.25, '2023-01-02 10:00:00', true);
insert into t9 values('group3', 2, 400.00, '2023-01-03 10:00:00', false);
insert into t9 values('group3', 2, 500.99, '2023-01-03 11:00:00', true);
insert into t9 values('group4', 3, 600.50, '2023-01-04 10:00:00', true);
create table t10(
    c1 STRING NULL,
    c0 INT NULL,
    c2 DECIMAL(10,2) NULL,
    c3 DATETIME NULL
) DUPLICATE KEY(c1, c0) DISTRIBUTED BY HASH(c1) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t10 values('valid1', 1, 100.50, '2023-01-01 10:00:00');
insert into t10 values(null, 1, 200.75, '2023-01-01 11:00:00');
insert into t10 values('valid1', 1, null, '2023-01-01 12:00:00');
insert into t10 values('valid1', 1, 100.50, null);
insert into t10 values('valid2', null, 300.25, '2023-01-02 10:00:00');
insert into t10 values('valid3', 2, null, null);
insert into t10 values(null, null, null, null);
create table t11(
    c0 INT,
    c1 STRING,
    c2 INT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 PROPERTIES('replication_num' = '1');
insert into t11 values(1, 'single1', 100);
insert into t11 values(2, 'single2', 200);
insert into t11 values(3, 'single3', 300);
CREATE TABLE `large_table` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"replication_num" = "1"
);
insert into large_table SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into large_table SELECT c0,c1,c2 from large_table, UNNEST([1,2]);
create table dummy as select max(c1)from large_table group by c0;