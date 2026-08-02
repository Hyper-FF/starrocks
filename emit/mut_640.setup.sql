CREATE TABLE left_t (
    k INT,
    dt DATETIME,
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
CREATE TABLE right_t (
    k INT,
    dt VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
INSERT INTO left_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (2, '2024-02-01 00:00:00', 'us-west', 'SFO'),
    (3, NULL, 'eu', 'LON');
INSERT INTO right_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (4, '2024-03-01 00:00:00', 'apac', 'TYO'),
    (5, NULL, NULL, 'BER');