CREATE TABLE IF NOT EXISTS `to_tera_date_test` (
  `d1` DATE,
  `d2` VARCHAR(1024)
)
DISTRIBUTED BY HASH(`d1`)
PROPERTIES(
  "replication_num" = "1"
);
INSERT INTO `to_tera_date_test`(d1, d2)
VALUES ('2023-04-01', NULL),
       ('2023-04-02', '2023-04-02 20:13:14'),
       ('2023-04-03', '2023-04-03 20:13:14');
drop table to_tera_date_test;
CREATE TABLE IF NOT EXISTS test_to_date_table1 (
id varchar(150) NOT NULL COMMENT '',
v1 varchar(32) NULL COMMENT ""
) ENGINE=olap PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("enable_persistent_index" = "true", "replication_num" = "1");
insert into test_to_date_table1 values('1','2023-10-11 00:00:01.030'), ('2','2023-10-11 00:00:01.031'), ('3','2023-10-13 00:00:01.031'), ('4','2023-10-14 00:00:01.031');
drop table test_to_date_table1;
CREATE TABLE test_to_date_table2 (
  k1 bigint null
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);
insert into test_to_date_table2 select generate_series from TABLE(generate_series(0, 50000));
drop table test_to_date_table2;