CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc INT AS (array_avg(array_data)) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( id BIGINT NOT NULL,  incr BIGINT AUTO_INCREMENT, array_data ARRAY<int> NOT NULL, mc BIGINT AS (incr) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc DOUBLE AS (array_avg(array_data)), mc_1 DOUBLE AS (mc) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc BIGINT AS (sum(id)) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc DOUBLE AS (array_avg(array_data)), job INT NOT NULL ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( mc INT AS (1) ) PRIMARY KEY (mc) DISTRIBUTED BY HASH(mc) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc DOUBLE AS (array_avg(array_data)) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc DOUBLE AS (array_avg(array_data)) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, [1,2], 0.0);
INSERT INTO t (id, array_data, mc) VALUES (1, [1,2], 0.0);
INSERT INTO t VALUES (1, [1,2]);
INSERT INTO t (id, array_data) VALUES (2, [3,4]);
CREATE TABLE t1 ( id BIGINT NOT NULL,  array_data ARRAY<int> NOT NULL, mc DOUBLE AS (array_avg(array_data)) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t1 VALUES (3, [10,20]);
INSERT INTO t1 (id, array_data) VALUES (4, [30,40]);
INSERT INTO t SELECT id, array_data FROM t1;
DROP TABLE t;
DROP TABLE t1;
CREATE TABLE t ( id BIGINT NOT NULL, job INT NOT NULL, incr BIGINT AUTO_INCREMENT, array_data ARRAY<int> NOT NULL ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2, DEFAULT, [1,2]);
ALTER TABLE t ADD COLUMN mc_1 DOUBLE AS (array_avg(array_data));
ALTER TABLE t ADD COLUMN mc_2 INT AS (job);
ALTER TABLE t ADD COLUMN mc_3 INT AS (array_avg(array_data));
ALTER TABLE t ADD COLUMN mc_3 DOUBLE AS AUTO_INCREMENT (array_avg(array_data));
ALTER TABLE t ADD COLUMN mc_3 DOUBLE AS (mc_1);
ALTER TABLE t ADD COLUMN mc_3 BIGINT AS (sum(id));
ALTER TABLE t MODIFY COLUMN mc_1 ARRAY<int> AS (array_sort(array_data));
ALTER TABLE t MODIFY COLUMN mc_1 INT AS (array_avg(array_data));
ALTER TABLE t MODIFY COLUMN mc_1 DOUBLE AS AUTO_INCREMENT (array_avg(array_data));
ALTER TABLE t MODIFY COLUMN mc_1 DOUBLE AS (mc_2);
ALTER TABLE t MODIFY COLUMN mc_1 BIGINT AS (sum(id));
ALTER TABLE t MODIFY COLUMN mc_1 BIGINT AS (incr);
ALTER TABLE t MODIFY COLUMN mc_2 INT;
ALTER TABLE t MODIFY COLUMN job BIGINT AS (id);
ALTER TABLE t DROP COLUMN mc_2;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL, name BIGINT NOT NULL, job INT NOT NULL, mc INT AS (job) ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
ALTER TABLE t ADD COLUMN newcol INT DEFAULT "0" AFTER mc;
ALTER TABLE t MODIFY COLUMN job BIGINT;
ALTER TABLE t DROP COLUMN job;
ALTER TABLE t MODIFY COLUMN name BIGINT AFTER mc;
ALTER TABLE t ADD COLUMN newcol INT DEFAULT "0";
ALTER TABLE t DROP COLUMN mc;
ALTER TABLE t MODIFY COLUMN job BIGINT;
ALTER TABLE t DROP COLUMN job;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL, mc BIGINT AS (id + 1) ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY HASH(id) BUCKETS 10 REFRESH ASYNC AS SELECT id, mc FROM t;
INSERT INTO t VALUES (1);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL, mc BIGINT AS (id) ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
ALTER TABLE t MODIFY COLUMN mc BIGINT AS (id + 10);
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
INSERT INTO t VALUES (4);
INSERT INTO t VALUES (5);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL, mc BIGINT AS (id) ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
ALTER TABLE t DROP COLUMN mc;
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
INSERT INTO t VALUES (4);
INSERT INTO t VALUES (5);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL, mc BIGINT AS (id) ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
ALTER TABLE t ADD COLUMN name BIGINT AS (id + 10);
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
INSERT INTO t VALUES (4);
INSERT INTO t VALUES (5);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1), (2), (3);
ALTER TABLE t ADD COLUMN (newcol1 BIGINT AS id * 10, newcol2 BIGINT AS id * 100);
ALTER TABLE t ADD COLUMN (newcol3 BIGINT DEFAULT "0", newcol4 BIGINT DEFAULT "0");
ALTER TABLE t ADD COLUMN (newcol5 BIGINT DEFAULT "0", newcol6 BIGINT AS id * 1000);
CREATE TABLE t ( id BIGINT NOT NULL, v1 BIGINT NOT NULL, v2 BIGINT NOT NULL, v3 BIGINT AS v2) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2, 3);
INSERT INTO t VALUES (1, 2, 3);
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol1 BIGINT AS id + name;
ALTER TABLE t ADD COLUMN newcol2 BIGINT AS id * 10 + name;
ALTER TABLE t ADD COLUMN newcol3 BIGINT AS id * 100 + name;
ALTER TABLE t DROP COLUMN newcol1;
ALTER TABLE t DROP COLUMN newcol2;
ALTER TABLE t DROP COLUMN newcol3;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol1 BIGINT AS id + name;
ALTER TABLE t ADD COLUMN newcol2 BIGINT AS id * 10 + name;
ALTER TABLE t ADD COLUMN newcol3 BIGINT AS id * 100 + name;
ALTER TABLE t DROP COLUMN newcol1;
ALTER TABLE t DROP COLUMN newcol2;
ALTER TABLE t DROP COLUMN newcol3;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL ) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol1 BIGINT AS id + name;
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol2 BIGINT AS id * 10 + name;
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol3 BIGINT AS id * 100 + name;
ALTER TABLE t DROP COLUMN newcol1;
ALTER TABLE t DROP COLUMN newcol2;
ALTER TABLE t DROP COLUMN newcol3;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL,  name BIGINT NOT NULL ) Unique KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol1 BIGINT AS id + name;
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol2 BIGINT AS id * 10 + name;
INSERT INTO t VALUES (1, 2);
ALTER TABLE t ADD COLUMN newcol3 BIGINT AS id * 100 + name;
ALTER TABLE t DROP COLUMN newcol1;
ALTER TABLE t DROP COLUMN newcol2;
ALTER TABLE t DROP COLUMN newcol3;
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL) PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1),(2),(3);
ALTER TABLE t ADD COLUMN newcol1 TINYINT AS 1;
ALTER TABLE t ADD COLUMN (newcol2 TINYINT AS 2, newcol3 TINYINT AS 3);
ALTER TABLE t ADD COLUMN (newcol4 TINYINT AS 4, newcol5 BIGINT AS id * 5, newcol6 TINYINT AS 6);
ALTER TABLE t ADD COLUMN (newcol7 BIGINT AS id * 7, newcol8 TINYINT AS 8, newcol9 BIGINT AS id * 9);
DROP TABLE t;
CREATE TABLE t ( id BIGINT NOT NULL) DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
INSERT INTO t VALUES (1),(2),(3);
ALTER TABLE t ADD COLUMN newcol1 TINYINT AS 1;
ALTER TABLE t ADD COLUMN (newcol2 TINYINT AS 2, newcol3 TINYINT AS 3);
ALTER TABLE t ADD COLUMN (newcol4 TINYINT AS 4, newcol5 BIGINT AS id * 5, newcol6 TINYINT AS 6);
ALTER TABLE t ADD COLUMN (newcol7 BIGINT AS id * 7, newcol8 TINYINT AS 8, newcol9 BIGINT AS id * 9);
DROP TABLE t;
CREATE TABLE t0 ( v1 BIGINT NOT NULL, v2 BIGINT NOT NULL, v3 BIGINT NOT NULL) DUPLICATE KEY (v1) DISTRIBUTED BY HASH(v1) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
alter table t0 add column testcol1 BIGINT after xxx, add column testcol2 BIGINT after testcol1;
alter table t0 add column testcol1 BIGINT after v1, add column testcol2 BIGINT after xxx;
alter table t0 add column testcol1 BIGINT after xxx, add column testcol2 BIGINT after xxx;
alter table t0 add column testcol1 BIGINT as v1 + 10, add column testcol2 BIGINT after testcol1;
alter table t0 add column testcol1 BIGINT after v1, add column testcol2 BIGINT after testcol1;
alter table t0 add column testcol3 BIGINT after v1, add column testcol4 BIGINT after v1;
alter table t0 add column testcol5 BIGINT, add column testcol6 BIGINT as v1 * 10;
CREATE TABLE `t` (
  `c1` BIGINT NOT NULL COMMENT "",
  `c2` BIGINT NOT NULL COMMENT "",
  `c3` BIGINT as `c1` + `c2`
) ENGINE=OLAP 
PRIMARY KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
INSERT INTO t values (1,1),(2,2),(3,3),(4,4);
ALTER TABLE t drop column c3;
INSERT INTO t values (1,1),(2,2),(3,3),(4,4);
ALTER TABLE t add column c4 BIGINT as c1 + c2 + 10;
CREATE TABLE `t_information_schema_generated_column_1` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AS k + 10
) ENGINE=OLAP 
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false"
);
CREATE TABLE `t_information_schema_generated_column_2` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` BIGINT AS k + 10
) ENGINE=OLAP 
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "false"
);
DROP TABLE t_information_schema_generated_column_1;
DROP TABLE t_information_schema_generated_column_2;
CREATE TABLE t_fix_adding_and_col_partial_update_conflict ( id BIGINT NOT NULL, v1 BIGINT NOT NULL, v2 BIGINT NOT NULL) Primary KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1", "replicated_storage" = "true", "fast_schema_evolution" = "true");
insert into t_fix_adding_and_col_partial_update_conflict values (1,2,3),(2,3,4),(3,4,5);
alter table t_fix_adding_and_col_partial_update_conflict add column newcol bigint as v2 * 100;
drop table t_fix_adding_and_col_partial_update_conflict;
CREATE TABLE `t1_create_like_error` (
                `k1` date,
                `k5` boolean,
                `k6` tinyint,
                `k7` smallint,
                `col_array` array<smallint> as [k5,k6,k7]
                )
                PRIMARY KEY(`k1`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_format" = "v2"
                );
INSERT INTO t2_create_like_error values(now(), 1, 2, 3);
DROP TABLE t1_create_like_error;
DROP TABLE t2_create_like_error;