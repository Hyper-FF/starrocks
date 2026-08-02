CREATE TABLE t_vi_cache (
    id bigint(20) NOT NULL,
    v1 ARRAY<FLOAT> NOT NULL,
    INDEX index_vector (v1) USING VECTOR (
        "index_type" = "hnsw",
        "dim" = "5",
        "metric_type" = "l2_distance",
        "is_vector_normed" = "false",
        "M" = "16",
        "efconstruction" = "40")
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t_vi_cache values
    (1, [1,2,3,4,5]),
    (2, [4,5,6,7,8]),
    (3, [2,3,4,5,6]),
    (4, [3,4,5,6,7]),
    (5, [5,6,7,8,9]);
DROP TABLE t_vi_cache;