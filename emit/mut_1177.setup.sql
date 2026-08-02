CREATE TABLE `test` (
  `id` int
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `test` (
  `id` int
)
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `test1` (
  `id` int
);
CREATE TABLE `test2` (
  `id` int
);
CREATE TABLE `test3` (
  `id` int
);
CREATE TABLE `test1` (
  `id` int
)
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE `test2` (
  `id` int
)
PROPERTIES (
 "replication_num" = "1"
);