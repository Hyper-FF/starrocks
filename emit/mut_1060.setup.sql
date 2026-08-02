CREATE TABLE IF NOT EXISTS t1
(
    `id` bigint(20) NULL,
    `k2` datetime NULL,
    `k3` varchar(32),
    `k4` int(11) NULL,
    `k5` bigint(20),
    `k6` double NULL,
    `k7` varchar(255) NULL
    ) ENGINE = OLAP
    DUPLICATE KEY(id, k2, k3)
    PARTITION BY RANGE(k2)(
    START ("2022-04-17") END ("2022-05-01") EVERY (INTERVAL 1 day))
    DISTRIBUTED BY HASH(id)
    PROPERTIES
(
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p"
);
INSERT INTO t1 values(1, '2022-04-17 00:00:00', 'k3', 1, 1, 1.0, 'k7'), (2, '2022-04-20 00:00:00', 'k4', 1, 1, 1.0, 'k7');
alter table t1 drop column id;
alter table t1 drop column k2;
alter table t1 drop column k3;
alter table t1 drop column k4;
alter table t1 drop column k5;
alter table t1 drop column k6;
alter table t1 drop column k7;