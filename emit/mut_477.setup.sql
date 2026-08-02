CREATE TABLE `t1` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t2` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 5
PROPERTIES (
"colocate_with" = "group1",
"replication_num" = "1",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4");
CREATE TABLE all_type (
    date_col DATE,
    datetime_col DATETIME,
    int_col int,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(10, 2),
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    array_col ARRAY<STRING>
) ENGINE=OLAP
  DUPLICATE KEY(date_col)
  COMMENT "OLAP"
  DISTRIBUTED BY HASH(date_col) BUCKETS 5
  PROPERTIES ("replication_num" = "1");
INSERT INTO t1 (c0, c1, c2, c3) VALUES
  (1, 'a', 'Value1', 10),
  (2, 'b', 'Value2', 20),
  (null, 'c', 'Value3', 30),
  (4, 'd', 'Value4', 40),
  (5, null, 'Value5', 50),
  (5, 'f', 'Value6', 60);
insert into all_type values('2021-01-01', '2021-01-01 12:00:01.123', 111111, -1.23, 1.54654, 120.26, '测试 test"', '测试\'', ['测试', '测试', 'abc'] );
insert into t2 select c0, 'a', c2, 60 from t1;
CREATE MATERIALIZED VIEW test_mv
REFRESH DEFERRED MANUAL
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
) as select  * from t1;
create table cities (
    city_id int NOT NULL,
     population int NOT NULL,
    city string NOT NULL
   ) PRIMARY KEY (city_id)
DISTRIBUTED BY HASH(city_id);
insert into cities(city_id, population, city) values(1, 2000,"beijing");
insert into cities(city_id, population, city) values(2, 2000,"shanghai");
insert into cities(city_id, population, city) values(3, 2000,"guangzhou");
insert into cities(city_id, population, city) values(4, 1000,"shenzhen");
insert into cities(city_id, population, city) values(5, 2000, "chengdu");