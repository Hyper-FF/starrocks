CREATE TABLE jitli (id INT, v LARGEINT NULL) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO jitli SELECT generate_series, 1 FROM TABLE(generate_series(1, 5000)) g;