CREATE TABLE IF NOT EXISTS t_cte_0 (
    v1 bigint NULL,
    v2 bigint NULL,
    v3 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
DISTRIBUTED BY HASH(v1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
CREATE TABLE IF NOT EXISTS t_cte_1 (
    v4 bigint NULL,
    v5 bigint NULL,
    v6 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(v4, v5, v6)
DISTRIBUTED BY HASH(v4) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO t_cte_0 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5);
INSERT INTO t_cte_1 VALUES (1, 10, 100), (2, 20, 200), (4, 40, 400);
DROP TABLE IF EXISTS t_cte_0;
DROP TABLE IF EXISTS t_cte_1;