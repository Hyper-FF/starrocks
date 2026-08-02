DROP TABLE IF EXISTS test_shared_scan_t;
CREATE TABLE test_shared_scan_t (
    k INT NOT NULL,
    v BIGINT NOT NULL,
    s VARCHAR(32) NOT NULL
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES("replication_num" = "1");
INSERT INTO test_shared_scan_t
SELECT generate_series, generate_series * 2, concat('r', generate_series)
FROM TABLE(generate_series(1, 300000));
DROP TABLE test_shared_scan_t;