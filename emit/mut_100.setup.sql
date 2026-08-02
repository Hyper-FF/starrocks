CREATE TABLE `t1` (
    `k1`  date,
    `k2`  int,
    `k3`  int
)
PROPERTIES ('replication_num' = '1');
INSERT INTO t1
WITH series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', s1.g1) as k1 , s1.g1, s2.g1
FROM series s1, series s2
WHERE s1.g1 <= s2.g1
ORDER BY s1.g1;
INSERT INTO t2
WITH series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', 300-s1.g1) as k1 , s1.g1, s2.g1
FROM series s1, series s2
WHERE s1.g1 <= s2.g1
ORDER BY s1.g1;
INSERT INTO t3
WITH 
series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', s1.g1), s1.g1, s2.g1
FROM series s1, series s2;
create table test_escaped_string (k1 string) properties("replication_num"="1");
insert into test_escaped_string select "aaaaa's";
insert into test_escaped_string select "bbbbbbb";
CREATE TABLE `t4` (
    `k1`  date,
    `k2`  int,
    `k3`  int
)
PROPERTIES ('replication_num' = '1');
INSERT INTO t4
SELECT * FROM t1
WHERE k2 % 3 != 2;
INSERT INTO t5
SELECT * FROM t2
WHERE k2 % 3 != 2;
INSERT INTO t6
SELECT * FROM t3
WHERE k2 % 3 != 2;
CREATE TABLE `t_histogram_all_columns` (
    `k1` int,
    `k2` varchar(20),
    `k3` date
)
PROPERTIES ('replication_num' = '1');
INSERT INTO t_histogram_all_columns VALUES
    (1, 'a', '2020-01-01'),
    (2, 'b', '2020-01-02'),
    (3, 'b', '2020-01-03'),
    (NULL, NULL, NULL);