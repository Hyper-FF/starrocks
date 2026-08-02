CREATE TABLE bt1 (
    id BIGINT,
    province VARCHAR(64) NOT NULL
)
PRIMARY KEY(id, province)
PARTITION BY LIST (province) (
    PARTITION psampledomain2ecom VALUES IN ("sample-domain2.com"),
    PARTITION psampledomainecom VALUES IN ("sample-domain.com")
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE bt2 (
    id BIGINT,
    province VARCHAR(64) NOT NULL
)
PRIMARY KEY(id, province)
PARTITION BY LIST (province) (
    PARTITION pother VALUES IN ("other.com")
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO bt1 VALUES (1, "sample-domain.com");
INSERT INTO bt1 VALUES (1, "sample-domain2.com");
INSERT INTO bt2 VALUES (1, "other.com");
CREATE MATERIALIZED VIEW mv_dup_partition_name_repro
PARTITION BY province
DISTRIBUTED BY HASH(province) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("replication_num" = "1")
AS
SELECT bt1.province, bt2.id
FROM bt1 LEFT JOIN bt2 ON bt1.province = bt2.province;
ALTER TABLE bt2 ADD PARTITION psampledomain2ecom VALUES IN ("sample-domain.com");
INSERT INTO bt2 VALUES (1, "sample-domain.com");