CREATE TABLE `uniq_t` (
    `k1` int NOT NULL,
    `k2` int NOT NULL,
    `v1` bigint,
    `v2` bigint
)
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY(k2, k1)
PROPERTIES ("replication_num" = "1");
insert into uniq_t values (1, 3, 10, 20), (2, 1, 30, 40), (3, 2, 50, 60);
alter table uniq_t add column k3 int key default '0';
insert into uniq_t values (4, 4, 70, 80, 1);
alter table uniq_t add column k4 int key default '0' after k1;
insert into uniq_t values (5, 2, 90, 100, 1, 2);
alter table uniq_t add column k5 int key default '0';
insert into uniq_t values (6, 3, 110, 120, 1, 2, 3);
alter table uniq_t order by (k1, k4, k5, k2, k3);
insert into uniq_t values (7, 1, 130, 140, 1, 2, 3);
alter table uniq_t add column k6 int key default '0';
insert into uniq_t values (8, 2, 4, 1, 2, 4, 150, 160);