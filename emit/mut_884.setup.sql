DROP TABLE if exists t0;
CREATE TABLE if not exists t0
 (
 c0 INT NOT NULL,
 c1 INT NOT NULL,
 c2 DECIMAL128(7, 2) NOT NULL,
 c3 VARCHAR(10) NOT NULL 
 ) ENGINE=OLAP
 DUPLICATE KEY(`c0`, `c1`, `c2` )
 COMMENT "OLAP"
 DISTRIBUTED BY HASH(`c0`, `c1` ) BUCKETS 32
 PROPERTIES(
 "replication_num" = "1",
 "in_memory" = "false",
 "storage_format" = "default" 
 );
INSERT INTO t0
(c0, c1, c2, c3)
VALUES
('9', '8', '-23765.20', 'foo1'),
('7', '1', '92426.92', 'foo6'),
('2', '1', '-96540.02', 'foo10'),
('7', '8', '-96540.02', 'foo1'),
('5', '3', '70459.31', 'foo10'),
('6', '1', '66032.48', 'foo9'),
('4', '2', '-99763.42', 'foo2'),
('1', '2', '92426.92', 'foo1'),
('8', '9', '73215.84', 'foo10'),
('5', '3', '45826.02', 'foo6');