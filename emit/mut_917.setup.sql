CREATE TABLE `pk_large_split` (
    `k1` BIGINT NOT NULL,
    `k2` INT NOT NULL,
    `v1` VARCHAR(500),
    `v2` BIGINT
)
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "lake_compaction_max_parallel" = "4"
);
INSERT INTO pk_large_split SELECT generate_series, generate_series, CONCAT(MD5(CAST(generate_series AS STRING)), MD5(CAST(generate_series*2 AS STRING)), MD5(CAST(generate_series*3 AS STRING)), MD5(CAST(generate_series*4 AS STRING)), MD5(CAST(generate_series*5 AS STRING)), MD5(CAST(generate_series*6 AS STRING)), MD5(CAST(generate_series*7 AS STRING)), MD5(CAST(generate_series*8 AS STRING)), MD5(CAST(generate_series*9 AS STRING)), MD5(CAST(generate_series*10 AS STRING))), generate_series * 10 FROM TABLE(generate_series(1, 50000));