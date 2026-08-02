CREATE TABLE rk (id INT, v VARCHAR(32))
DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num"="1");
INSERT INTO rk SELECT generate_series, lpad(cast(generate_series AS varchar), 10, '0')
FROM TABLE(generate_series(1, 100000));