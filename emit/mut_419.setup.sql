CREATE TABLE IF NOT EXISTS test_order (
id varchar(150) NOT NULL COMMENT '',
reset_period_data varchar(32) NULL COMMENT ""
) ENGINE=olap PRIMARY KEY (id) COMMENT '' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("enable_persistent_index" = "true", "replication_num" = "1");
insert into test_order values('1','2023-10-11 00:00:01.030');