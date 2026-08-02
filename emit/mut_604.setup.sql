CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) PARTITION BY RANGE(event_day)(PARTITION p1 VALUES LESS THAN ("2020-01-31")) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "3");
insert into ss values('2020-01-14', 2);
CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) PARTITION BY RANGE(event_day)(PARTITION p1 VALUES LESS THAN ("2020-01-31")) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "3");
insert into ss values('2022-01-14', 2);
CREATE TABLE ss (k1 bigint NOT NULL, k2 bigint NOT NULL, k3 bigint NOT NULL) duplicate key (k1) distributed by hash(k2) buckets 1 PROPERTIES("replication_num" = "3");
insert into ss values(null, null, null);