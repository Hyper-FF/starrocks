CREATE TABLE test_table_1 (
    id INT,
    value VARCHAR(10)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE test_table_2 (
    id INT,
    value VARCHAR(10)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO test_table_1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO test_table_2 VALUES (4, 'd'), (5, 'e'), (6, 'f');
CREATE TABLE join_table (
    id INT,
    detail VARCHAR(10)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO join_table VALUES (1, 'info1'), (5, 'info5');
CREATE TABLE alias_table_1 (
    col1 INT,
    col2 VARCHAR(10)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE alias_table_2 (
    col1 INT,
    col2 VARCHAR(10)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO alias_table_1 VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
INSERT INTO alias_table_2 VALUES (4, 'delta'), (5, 'epsilon');
CREATE TABLE diff_table_1 (
    a INT,
    b VARCHAR(10)
) DUPLICATE KEY(a)
DISTRIBUTED BY HASH(a) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE diff_table_2 (
    x INT,
    y VARCHAR(10)
) DUPLICATE KEY(x)
DISTRIBUTED BY HASH(x) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO diff_table_1 VALUES (1, 'one'), (2, 'two');
INSERT INTO diff_table_2 VALUES (3, 'three'), (4, 'four');
CREATE TABLE alias_agg_table_1 (
    c1 INT,
    c2 VARCHAR(10)
) DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE alias_agg_table_2 (
    c1 INT,
    c2 VARCHAR(10)
) DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO alias_agg_table_1 VALUES (1, 'x'), (2, 'y'), (3, 'z');
INSERT INTO alias_agg_table_2 VALUES (4, 'p'), (5, 'q'), (6, 'r');
CREATE TABLE exclude_col_table_1 (
    col1 INT,
    col2 INT,
    col3 VARCHAR(10)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE exclude_col_table_2 (
    col1 INT,
    col2 INT,
    col3 VARCHAR(10)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO exclude_col_table_1 VALUES (1, 10, 'foo'), (2, 20, 'bar'), (3, 30, 'baz');
INSERT INTO exclude_col_table_2 VALUES (4, 40, 'qux'), (5, 50, 'quux'), (6, 60, 'corge');
CREATE TABLE join_cond_table_1 (
    id INT,
    detail VARCHAR(10)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE join_cond_table_2 (
    id INT,
    detail VARCHAR(10)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO join_cond_table_1 VALUES (1, 'info1'), (3, 'info3');
INSERT INTO join_cond_table_2 VALUES (2, 'info2'), (4, 'info4');
CREATE TABLE hash_dist_table (
    key1 INT,
    key2 VARCHAR(10)
) DUPLICATE KEY(key1)
DISTRIBUTED BY HASH(key1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE broadcast_dist_table (
    key1 INT,
    key2 VARCHAR(10)
) DUPLICATE KEY(key1)
DISTRIBUTED BY HASH(key1) BUCKETS 1
PROPERTIES('replication_num' = '1');
INSERT INTO hash_dist_table VALUES (1, 'h1'), (2, 'h2');
INSERT INTO broadcast_dist_table VALUES (3, 'b1'), (4, 'b2');
CREATE TABLE func_table_1 (
    id INT,
    name VARCHAR(20),
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE func_table_2 (
    id INT,
    name VARCHAR(20),
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO func_table_1 VALUES (1, 'A', 100), (2, 'B', 200), (3, 'C', NULL);
INSERT INTO func_table_2 VALUES (4, 'D', 50), (5, 'E', NULL), (6, 'F', 300);
CREATE TABLE agg_table_1 (
    id INT,
    category VARCHAR(20),
    count INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE agg_table_2 (
    id INT,
    category VARCHAR(20),
    count INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO agg_table_1 VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30);
INSERT INTO agg_table_2 VALUES (4, 'A', 40), (5, 'B', 50), (6, 'C', 60);
CREATE TABLE compute_table_1 (
    id INT,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE compute_table_2 (
    id INT,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO compute_table_1 VALUES (1, 100), (2, 200), (3, 300);
INSERT INTO compute_table_2 VALUES (4, 400), (5, 500), (6, 600);
CREATE TABLE filter_table_1 (
    id INT,
    type VARCHAR(20),
    amount INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE filter_table_2 (
    id INT,
    type VARCHAR(20),
    amount INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO filter_table_1 VALUES (1, 'electronics', 100), (2, 'furniture', 200), (3, 'fashion', 300);
INSERT INTO filter_table_2 VALUES (4, 'electronics', 400), (5, 'furniture', 500), (6, 'fashion', 600);
CREATE TABLE nested_int_table_1 (
    id INT,
    group_id string
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE nested_int_table_2 (
    id INT,
    group_id string
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO nested_int_table_1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO nested_int_table_2 VALUES (4, 40), (5, 50), (6, 60);
CREATE TABLE cte_table_1 (
    id INT,
    value VARCHAR(20)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE cte_table_2 (
    id INT,
    value VARCHAR(20)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE join_table_1 (
    id INT,
    detail VARCHAR(20)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO cte_table_1 VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO cte_table_2 VALUES (4, 'D'), (5, 'E'), (6, 'F');
INSERT INTO join_table_1 VALUES (1, 'info1'), (5, 'info5');
CREATE TABLE source_table_1 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE source_table_2 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE join_table_2 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO source_table_1 VALUES (1, 'X'), (2, 'Y'), (3, 'Z');
INSERT INTO source_table_2 VALUES (4, 'P'), (5, 'Q'), (6, 'R');
INSERT INTO join_table_2 VALUES (1, 'J1'), (4, 'J4');
CREATE TABLE ctas_result
DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1') AS
SELECT st1.col1, jt.col2
FROM (
    SELECT col1 FROM source_table_1
    UNION ALL
    SELECT col1 FROM source_table_2
) st1
JOIN join_table_2 jt ON st1.col1 = jt.col1;
CREATE TABLE agg_cte_table_1 (
    id INT,
    category VARCHAR(20),
    amount INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE agg_cte_table_2 (
    id INT,
    category VARCHAR(20),
    amount INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO agg_cte_table_1 VALUES (1, 'Electronics', 300), (2, 'Furniture', 200), (3, 'Fashion', 700);
INSERT INTO agg_cte_table_2 VALUES (4, 'Electronics', 500), (5, 'Furniture', 100), (6, 'Fashion', 300);
CREATE TABLE multi_union_table_1 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE multi_union_table_2 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE multi_union_table_3 (
    col1 INT,
    col2 VARCHAR(20)
) DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO multi_union_table_1 VALUES (1, 'Alpha'), (2, 'Beta');
INSERT INTO multi_union_table_2 VALUES (3, 'Gamma'), (4, 'Delta');
INSERT INTO multi_union_table_3 VALUES (5, 'Epsilon'), (6, 'Zeta');
CREATE TABLE ctas_result_2
DUPLICATE KEY(col1)
DISTRIBUTED BY HASH(col1) BUCKETS 3
PROPERTIES('replication_num' = '1') AS
SELECT col1, col2 FROM (
    SELECT col1, col2 FROM multi_union_table_1
    UNION ALL
    SELECT col1, col2 FROM multi_union_table_2
    UNION ALL
    SELECT col1, col2 FROM multi_union_table_3
) t;
CREATE TABLE large_table_1 (
    id INT,
    value VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE large_table_2 (
    id INT,
    value VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO large_table_1 VALUES
(1, 'val1'), (2, 'val2'), (3, 'val3'),
(4, 'val4'), (5, 'val5'), (6, 'val6'),
(7, 'val7'), (8, 'val8'), (9, 'val9'),
(10, 'val10'), (11, 'val11'), (12, 'val12'),
(13, 'val13'), (14, 'val14'), (15, 'val15');
INSERT INTO large_table_1
SELECT id + 15, CONCAT('val', id + 15) FROM large_table_1;
INSERT INTO large_table_2
SELECT id + 30, CONCAT('str', id + 30) FROM large_table_1;
CREATE TABLE broadcast_table_1 (
    id INT,
    `key` VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE broadcast_table_2 (
    id INT,
    `key` VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO broadcast_table_1
SELECT id, CONCAT('dict_broad_', id) FROM large_table_1;
INSERT INTO broadcast_table_2
SELECT id + 300, CONCAT('dict_broad_', id + 300) FROM large_table_1;
CREATE TABLE dict_filter_table_1 (
    id INT,
    `text` VARCHAR(100)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
CREATE TABLE dict_filter_table_2 (
    id INT,
    `text` VARCHAR(100)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO dict_filter_table_1
SELECT id, CONCAT('filter_case_', id) FROM large_table_1;
INSERT INTO dict_filter_table_2
SELECT id + 150, CONCAT('filter_case_', id + 150) FROM large_table_1;