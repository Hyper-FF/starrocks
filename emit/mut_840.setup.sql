CREATE TABLE t1 (
      id BIGINT,
      province VARCHAR(64) not null,
      age SMALLINT,
      dt VARCHAR(10)
)
PRIMARY KEY(id, province)
PARTITION BY LIST (province) (
     PARTITION p1 VALUES IN ("beijing", "chongqing") ,
     PARTITION p2 VALUES IN ("guangdong") 
)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES (1, 'beijing', 20, '2024-01-01'), (2, 'guangdong', 20, '2024-01-01'), (3, 'guangdong', 20, '2024-01-02');
CREATE TABLE t2 (
      id BIGINT,
      province VARCHAR(64) not null,
      age SMALLINT,
      dt VARCHAR(10)
)
PRIMARY KEY(id, province)
PARTITION BY LIST (province) (
     PARTITION p1 VALUES IN ("chongqing"),
     PARTITION p2 VALUES IN ("guangdong"),
     PARTITION p3 VALUES IN ("beijing")
)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t2 VALUES (1, 'beijing', 20, '2024-01-01'), (2, 'guangdong', 20, '2024-01-01'), (3, 'guangdong', 20, '2024-01-02');
CREATE TABLE t3 (
      id BIGINT,
      province VARCHAR(64) not null,
      age SMALLINT,
      dt VARCHAR(10) not null
)
DUPLICATE KEY(id)
PARTITION BY LIST (province, dt) (
     PARTITION p1 VALUES IN (("beijing", "2024-01-01"))  ,
     PARTITION p2 VALUES IN (("guangdong", "2024-01-01")), 
     PARTITION p3 VALUES IN (("beijing", "2024-01-02"))  ,
     PARTITION p4 VALUES IN (("guangdong", "2024-01-02")) 
)
DISTRIBUTED BY RANDOM;
INSERT INTO t3 VALUES (1, 'beijing', 20, '2024-01-01'), (2, 'guangdong', 20, '2024-01-01'), (3, 'guangdong', 20, '2024-01-02');
create materialized view test_mv1
partition by province 
distributed by hash(dt, province) buckets 10 
PROPERTIES (
"replication_num" = "1"
) 
as select dt, province, sum(age) from t1 group by dt, province;
INSERT INTO t1 VALUES (2, 'beijing', 20, '2024-01-01');
create materialized view test_mv1
partition by province 
distributed by hash(dt, province) buckets 10 
PROPERTIES (
"replication_num" = "1"
) 
as select dt, province, sum(age) from t2 group by dt, province;
INSERT INTO t2 VALUES (2, 'beijing', 20, '2024-01-01');
create materialized view test_mv1
partition by dt
REFRESH DEFERRED MANUAL
distributed by hash(dt, province) buckets 10 
PROPERTIES (
"replication_num" = "1"
) 
as select dt, province, sum(age) from t3 group by dt, province;
INSERT INTO t3 VALUES (2, 'beijing', 20, '2024-01-01');
drop table t1;
drop table t2;
drop table t3;