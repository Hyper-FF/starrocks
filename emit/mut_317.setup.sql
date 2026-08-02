CREATE TABLE t_range_date (
    dt DATE NOT NULL,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES [('2024-01-01'), ('2024-01-10')),
    PARTITION p2 VALUES [('2024-01-10'), ('2024-01-20')),
    PARTITION p3 VALUES [('2024-01-20'), ('2024-02-01'))
)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_date VALUES
    ('2024-01-01', 1), ('2024-01-05', 2), ('2024-01-09', 3),
    ('2024-01-10', 4), ('2024-01-15', 5), ('2024-01-19', 6),
    ('2024-01-20', 7), ('2024-01-25', 8), ('2024-01-31', 9);
drop table t_range_date force;
CREATE TABLE t_range_int (
    k INT NOT NULL,
    v STRING
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES [('0'), ('100')),
    PARTITION p2 VALUES [('100'), ('200')),
    PARTITION p3 VALUES [('200'), ('300'))
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_int VALUES
    (10, 'a'), (50, 'b'), (99, 'c'),
    (100, 'd'), (150, 'e'), (199, 'f'),
    (200, 'g'), (250, 'h'), (299, 'i');
drop table t_range_int force;
CREATE TABLE t_range_null_date (
    dt DATE,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ("2024-01-01"),
    PARTITION p2 VALUES LESS THAN ("2024-02-01"),
    PARTITION p3 VALUES LESS THAN ("2024-03-01")
)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_null_date VALUES
    (NULL, 1), (NULL, 2),
    ('2024-01-15', 3),
    ('2024-02-15', 4);
drop table t_range_null_date force;
CREATE TABLE t_range_null_tinyint (
    k TINYINT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES [("-128"), ("0")),
    PARTITION p2 VALUES [("0"), ("5")),
    PARTITION p3 VALUES [("5"), ("127"))
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_null_tinyint VALUES
    (NULL, 1), (NULL, 2),
    (-10, 3), (-1, 4),
    (0, 5), (25, 6),
    (50, 7), (100, 8);
drop table t_range_null_tinyint force;
CREATE TABLE t_range_null_smallint (
    k SMALLINT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY RANGE(k) (
     PARTITION p1 VALUES LESS THAN ('-3200'),
     PARTITION p2 VALUES LESS THAN ('0'),
     PARTITION p3 VALUES LESS THAN ('32000')
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_null_smallint VALUES
    (NULL, 1),
    (-32100, 2),
    (-100, 3),
    (100, 4);
drop table t_range_null_smallint force;
CREATE TABLE t_list_int (
    k INT NOT NULL,
    v STRING
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY LIST(k) (
    PARTITION p1 VALUES IN ('1', '2', '3'),
    PARTITION p2 VALUES IN ('4', '5', '6'),
    PARTITION p3 VALUES IN ('7', '8', '9')
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_list_int VALUES
    (1, 'a'), (2, 'b'), (3, 'c'),
    (4, 'd'), (5, 'e'), (6, 'f'),
    (7, 'g'), (8, 'h'), (9, 'i');
drop table t_list_int force;
CREATE TABLE t_list_null (
    k INT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY LIST(k) (
    PARTITION p1 VALUES IN ('1', '2', '3'),
    PARTITION p2 VALUES IN ('4', '5', '6', NULL)
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_list_null VALUES
    (NULL, 1), (NULL, 2),
    (1, 3), (2, 4),
    (4, 5), (5, 6);
drop table t_list_null force;
CREATE TABLE t_list_null_mixed (
    k INT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY LIST(k) (
    PARTITION p0 VALUES IN (NULL, '0', '1'),
    PARTITION p1 VALUES IN ('2', '3'),
    PARTITION p2 VALUES IN ('4', '5')
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_list_null_mixed VALUES
    (NULL, 1), (0, 2), (1, 3),
    (2, 4), (3, 5),
    (4, 6), (5, 7);
drop table t_list_null_mixed force;
CREATE TABLE t_list_multi_null (
    c1 INT,
    c2 INT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(c1)
PARTITION BY LIST(c1, c2) (
    PARTITION p0 VALUES IN ((NULL, NULL)),
    PARTITION p1 VALUES IN (('1', '10'), ('2', '20')),
    PARTITION p2 VALUES IN (('3', '30'), ('4', '40'))
)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_list_multi_null VALUES
    (NULL, NULL, 1),
    (1, 10, 2), (2, 20, 3),
    (3, 30, 4), (4, 40, 5);
drop table t_list_multi_null force;
CREATE TABLE t_limit_test (
    k INT NOT NULL,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES [('0'), ('100')),
    PARTITION p2 VALUES [('100'), ('200')),
    PARTITION p3 VALUES [('200'), ('300'))
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_limit_test VALUES (50, 1), (150, 2), (250, 3);
drop table t_limit_test force;
CREATE TABLE t_range_cast (
    dt DATE NOT NULL,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p2 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p3 VALUES [('2024-03-01'), ('2024-04-01'))
)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_range_cast VALUES
    ('2024-01-15', 1),
    ('2024-02-15', 2),
    ('2024-03-15', 3);
drop table t_range_cast force;
CREATE TABLE t_multi_pred (
    dt DATE NOT NULL,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES [('2024-01-01'), ('2024-01-10')),
    PARTITION p2 VALUES [('2024-01-10'), ('2024-01-20')),
    PARTITION p3 VALUES [('2024-01-20'), ('2024-02-01'))
)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_multi_pred VALUES
    ('2024-01-05', 1), ('2024-01-08', 2),
    ('2024-01-12', 3), ('2024-01-18', 4),
    ('2024-01-22', 5), ('2024-01-30', 6);
drop table t_multi_pred force;
CREATE TABLE t_open_bound (
    k INT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(k)
PARTITION BY RANGE(k) (
    PARTITION p1 VALUES [('0'), ('100')),
    PARTITION p2 VALUES [('100'), ('2100000000'))
)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_open_bound VALUES
    (50, 1), (99, 2),
    (100, 3), (999, 4);
drop table t_open_bound force;
CREATE TABLE t_null_combined (
    dt DATE,
    category INT,
    v INT
) ENGINE=OLAP
DUPLICATE KEY(dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ("2024-01-01"),
    PARTITION p2 VALUES LESS THAN ("2024-02-01"),
    PARTITION p3 VALUES LESS THAN ("2024-03-01")
)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_null_combined VALUES
    (NULL, 1, 10), (NULL, 2, 20),
    ('2024-01-15', 1, 30),
    ('2024-02-15', 2, 40);
drop table t_null_combined force;