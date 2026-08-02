CREATE TABLE `t1` (
   `id` varchar(36),
   `location_id` varchar(36),
   `location_id_hash` int,
   `source_id` varchar(36),
   `person_id` varchar(36)
) ENGINE=OLAP
PRIMARY KEY(`id`,`location_id`,`location_id_hash`)
PARTITION BY (`location_id_hash`)
DISTRIBUTED BY HASH(`id`) BUCKETS 3
PROPERTIES (
   "replication_num" = "1"
);
INSERT INTO t1 VALUES (1, 'beijing', 20, 'a', 'a1'), (2, 'guangdong', 30, 'b', 'b1'), (3, 'guangdong', 20, 'c', 'c1');
create materialized view test_mv1
PARTITION BY `location_id_hash`
DISTRIBUTED BY HASH(`id`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
) 
as select `id`, `location_id`, `location_id_hash` from `t1`;
INSERT INTO t1 VALUES (3, 'guangdong', 30, 'c', 'c1');