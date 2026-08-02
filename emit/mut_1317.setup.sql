DROP TABLE IF EXISTS window_skew_mvc_t1;
CREATE TABLE window_skew_mvc_t1 (
    pk INT,
    v1 INT,
    v2 INT
) engine=OLAP
DUPLICATE KEY(pk)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO window_skew_mvc_t1  VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO window_skew_mvc_t1  VALUES (100, 4, 4),(100, 5, 5), (100, 6, 6), (100, 7, 7),
                                       (100, 8, 8), (100, 9, 9), ( 100, 10, 10),
                                       (100, 11, 11), (100, 12, 12), (100, 13, 13);
DROP TABLE IF EXISTS window_skew_mvc_t1;