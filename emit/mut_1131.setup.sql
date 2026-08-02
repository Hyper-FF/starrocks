CREATE TABLE `agg_t` (
    `k1` int NOT NULL,
    `k2` int NOT NULL,
    `v1` bigint SUM DEFAULT '0',
    `v2` bigint SUM DEFAULT '0'
)
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
ORDER BY(k2, k1)
PROPERTIES ("replication_num" = "1");
insert into agg_t values (1, 3, 10, 20), (2, 1, 30, 40), (3, 2, 50, 60);
alter table agg_t add column k3 int default '0';
insert into agg_t values (4, 4, 70, 80, 1);
insert into agg_t values (1, 3, 0, 5, 5), (2, 1, 0, 10, 10);
alter table agg_t add column k4 int default '0' after k1;
insert into agg_t values (5, 2, 90, 100, 1, 2);
insert into agg_t values (1, 0, 3, 0, 5, 5), (2, 0, 1, 0, 10, 10);
alter table agg_t add column k5 int default '0';
insert into agg_t values (6, 3, 110, 120, 1, 2, 3);
insert into agg_t values (1, 0, 3, 0, 0, 5, 5), (2, 0, 1, 0, 0, 10, 10);
alter table agg_t order by (k1, k4, k5, k2, k3);
insert into agg_t values (7, 1, 130, 140, 1, 2, 3);
insert into agg_t values (1, 0, 3, 0, 0, 5, 5), (2, 0, 1, 0, 0, 10, 10);
alter table agg_t add column k6 int default '0';
insert into agg_t values (8, 2, 4, 1, 2, 4, 150, 160);
insert into agg_t values (1, 0, 3, 0, 0, 0, 5, 5), (2, 0, 1, 0, 0, 0, 10, 10);