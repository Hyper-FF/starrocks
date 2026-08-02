CREATE TABLE `t1` (
  `id` int(11) NULL,
  `pt` date NOT NULL,
  `gmv` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
PARTITION BY date_trunc('day', pt)
DISTRIBUTED BY HASH(`pt`)
PROPERTIES (
"replication_num" = "1"
);
insert into t1 values(2,'2023-03-07',1), (2,'2023-03-08',3), (2,'2023-03-11',10);
CREATE MATERIALIZED VIEW `test_mv1` 
PARTITION BY (`pt`)
DISTRIBUTED BY RANDOM
REFRESH DEFERRED ASYNC START("2024-03-08 03:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
"partition_refresh_number" = "1"
)
AS SELECT `pt`, `id`, sum(gmv) AS `sum_gmv`, count(gmv) AS `count_gmv`
FROM `t1`
GROUP BY `pt`, `id`;