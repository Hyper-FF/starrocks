CREATE TABLE test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO test VALUES (3, 'Zac'), (4, 'Tom');
ALTER TABLE test ADD COLUMN age INT;
ALTER TABLE test MODIFY COLUMN name STRING;
INSERT INTO test (id, name, age) VALUES (5, 'Eve', 30);
CREATE TABLE test2 (id INT, age INT) ENGINE=OLAP PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
INSERT INTO test2 VALUES (1, 18), (2, 20);