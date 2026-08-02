CREATE TABLE t ( id BIGINT NOT NULL ,  name BIGINT NOT NULL, job1 BIGINT AUTO_INCREMENT, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL ,  name BIGINT NOT NULL, job1 BIGINT NOT NULL AUTO_INCREMENT, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
DROP TABLE t;
CREATE TABLE t ( id BIGINT AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT AUTO_INCREMENT, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
DROP TABLE t;
CREATE TABLE t ( id INT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL AUTO_INCREMENT, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL AUTO_INCREMENT, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true");
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id, name,job1,job2) VALUES (DEFAULT, 1,1,1),(2,2,2);
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,3,3,3),(DEFAULT,4,4,4);
INSERT INTO t (id,name,job1,job2) VALUES (100,5,5,5);
INSERT INTO t (id,name,job1,job2) VALUES (101,6,6,6),(DEFAULT,7,7,7);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id,name,job1,job2) VALUES (1,1,1,1),(100000,1,2,2);
INSERT INTO t (id, name,job1,job2) VALUES (DEFAULT, 1,100,100);
INSERT INTO t (id,name,job1,job2) VALUES (100000,1,100,100);
INSERT INTO t (id,name,job1,job2) VALUES (100000,1,200,200), (10,10,99,99);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL, job1 BIGINT NOT NULL AUTO_INCREMENT, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id,name,job2) VALUES (1,1,1),(2,2,2);
INSERT INTO t (id,name,job1, job2) VALUES (3,3,DEFAULT,3),(4,4,DEFAULT,4);
INSERT INTO t (id,name,job1, job2) VALUES (5,5,100,5);
INSERT INTO t (id,name,job1, job2) VALUES (6,6,101,6),(7,7,DEFAULT,7);
DROP TABLE t;
CREATE TABLE t1 ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
CREATE TABLE t2 ( id BIGINT NOT NULL,  name BIGINT NOT NULL AUTO_INCREMENT, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
CREATE TABLE t3 ( id BIGINT NOT NULL,  name BIGINT NOT NULL, job1 BIGINT NOT NULL AUTO_INCREMENT, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
CREATE TABLE t4 ( id BIGINT NOT NULL,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL AUTO_INCREMENT) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t1 (id,name,job1,job2) VALUES (DEFAULT,1,1,1);
INSERT INTO t2 (id,name,job1,job2) VALUES (1,DEFAULT,1,1);
INSERT INTO t3 (id,name,job1,job2) VALUES (1,1,DEFAULT,1);
INSERT INTO t4 (id,name,job1,job2) VALUES (1,1,1,DEFAULT);
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
CREATE TABLE t ( id BIGINT NOT NULL AUTO_INCREMENT,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id,name,job1,job2) values (DEFAULT,0,0,0);
INSERT INTO t (id,name,job1,job2) values (DEFAULT,1,1,1);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL, job1 BIGINT NOT NULL AUTO_INCREMENT, job2 BIGINT NOT NULL) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id,name,job1,job2) values (0,0,DEFAULT,0);
INSERT INTO t (id,name,job1,job2) values (1,1,DEFAULT,1);
INSERT INTO t (id,name,job1,job2) values (2,2,DEFAULT,2);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL AUTO_INCREMENT) Primary KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
ALTER TABLE t MODIFY COLUMN job2 BIGINT;
ALTER TABLE t DROP COLUMN job2;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL AUTO_INCREMENT, job1 BIGINT NULL, job2 BIGINT NULL) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t (id, name, job1, job2) VALUES (1,DEFAULT,NULL,2);
INSERT INTO t (id, name, job1, job2) VALUES (1,NULL,NULL,2);
INSERT INTO t VALUES (1,NULL,NULL,2);
DROP TABLE t;
CREATE TABLE t1 ( id BIGINT NOT NULL, idx BIGINT AUTO_INCREMENT )
Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
CREATE TABLE t2 ( id BIGINT NOT NULL, idx BIGINT NULL )
Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t2 VALUES (1, NULL), (2, NULL);
INSERT INTO t1 properties ("max_filter_ratio" = "1") SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT id, idx FROM t2;
INSERT INTO t1 properties ("max_filter_ratio" = "1") SELECT 1, NULL;
INSERT INTO t2 VALUES (10, 1), (20, 2);
DROP TABLE t1;
CREATE TABLE t1 ( id BIGINT NOT NULL, idx BIGINT AUTO_INCREMENT )
Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT id, idx FROM t2;
INSERT INTO t1 (id) SELECT id FROM t2;
DROP TABLE t1;
CREATE TABLE t1 ( id BIGINT NOT NULL, idx BIGINT AUTO_INCREMENT )
Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage"="true");
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ("max_filter_ratio" = "1") SELECT id, idx FROM t2;
INSERT INTO t1 (id) SELECT id FROM t2;
DROP TABLE t1;
DROP TABLE t2;
CREATE TABLE `t_auto_increment_incorrect_col_id` (
  `k1` BIGINT NOT NULL COMMENT "",
  `k2` string default "abc" COMMENT "",
  `k3` BIGINT AUTO_INCREMENT COMMENT "",
  `k4` string default "bcd"COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
INSERT INTO t_auto_increment_incorrect_col_id VALUES (1, DEFAULT, DEFAULT, DEFAULT);
DROP TABLE t_auto_increment_incorrect_col_id;
CREATE TABLE `t_auto_increment_partial_update_only` (
  `k1` BIGINT NOT NULL COMMENT "",
  `k2` BIGINT NOT NULL COMMENT "",
  `k3` BIGINT NOT NULL COMMENT "",
  `k4` BIGINT AUTO_INCREMENT COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);
INSERT INTO t_auto_increment_partial_update_only VALUES (1, 2, 3, DEFAULT);
DROP TABLE t_auto_increment_partial_update_only;
CREATE TABLE `t_auto_increment_insert_partial_update` (
  `k` STRING NOT NULL COMMENT "",
  `v1` BIGINT AUTO_INCREMENT,
  `created` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true"
);
insert into t_auto_increment_insert_partial_update (k) values (1);
insert into t_auto_increment_insert_partial_update (k) values (1),(2);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3),(4);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3),(4);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3),(4);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3),(4);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(3),(4);
insert into t_auto_increment_insert_partial_update (k) values (1),(2),(5);
DROP TABLE t_auto_increment_insert_partial_update force;
CREATE TABLE `t_auto_increment_insert_partial_update` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT "",
  `ts` bigint(20) NOT NULL COMMENT "",
  `testString` String NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`, `ts`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1 
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into t_auto_increment_insert_partial_update (ts, testString) select 100, "abc";
insert into t_auto_increment_insert_partial_update (ts, testString) select 100, "abc";
insert into t_auto_increment_insert_partial_update (ts, testString) select 100, "abc";
DROP TABLE t_auto_increment_insert_partial_update force;
CREATE TABLE `t_auto_increment_partial_update_column_upsert` (
  `k`  BIGINT NOT NULL COMMENT "",
  `v1` BIGINT AUTO_INCREMENT,
  `v2` BIGINT,
  `v3` BIGINT
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "1"
);
INSERT INTO t_auto_increment_partial_update_column_upsert VALUES (1, DEFAULT, 3, 4);
INSERT INTO t_auto_increment_partial_update_column_upsert VALUES (1, 300, 20, 30), (2, 301, 40 ,50);
DROP TABLE t_auto_increment_partial_update_column_upsert;
CREATE TABLE `t_alter_auto_increment_counter` (
  `k`  BIGINT NOT NULL COMMENT "",
  `v1` BIGINT AUTO_INCREMENT
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "1"
);
INSERT INTO t_alter_auto_increment_counter VALUES (1, 1);
INSERT INTO t_alter_auto_increment_counter VALUES (2, 2);
ALTER TABLE t_alter_auto_increment_counter AUTO_INCREMENT = 300;
INSERT INTO t_alter_auto_increment_counter VALUES (3, DEFAULT);
DROP TABLE t_alter_auto_increment_counter;
CREATE TABLE `t_auto_increment_partial_update_column_upsert_2` (
  `k`  BIGINT NOT NULL COMMENT "",
  `v1` BIGINT,
  `v2` BIGINT,
  `v3` BIGINT AUTO_INCREMENT,
  `v4` BIGINT
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "1"
);
INSERT INTO t_auto_increment_partial_update_column_upsert_2 VALUES (1, 2, 3, DEFAULT, 4);
INSERT INTO t_auto_increment_partial_update_column_upsert_2 VALUES (1, 300, 20, DEFAULT, 30), (2, 301, 40, DEFAULT, 50);
DROP TABLE t_auto_increment_partial_update_column_upsert_2;