CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t1 VALUES (1,'Alice',25),(2,'Bob',30),(3,'Charlie',35);
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100)
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
drop table t1;
CREATE TABLE t1 (
    id INT NOT NULL,
    name VARCHAR(100),
    age INT
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");