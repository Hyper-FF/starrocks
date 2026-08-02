CREATE TABLE `test_normal` (
  `id` int
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `test_default` (
  `id` int
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `test_timeout` (
  `id` int
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);