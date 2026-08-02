DROP TABLE IF EXISTS window_skew_t1;
CREATE TABLE window_skew_t1 (
    id BIGINT AUTO_INCREMENT ,
    pk INT,
    v1 INT,
    v2 INT
)
PRIMARY KEY(id)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO window_skew_t1 (pk, v1,v2 ) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO window_skew_t1 (pk, v1,v2 ) VALUES (NULL, 4, 4),(NULL, 5, 5), (NULL, 6, 6), (NULL, 7, 7),
                                            (NULL, 8, 8), (NULL, 9, 9), ( NULL, 10, 10),
                                            (NULL, 11, 11), (NULL, 12, 12), (NULL, 13, 13);
DROP TABLE IF EXISTS window_skew_t1;