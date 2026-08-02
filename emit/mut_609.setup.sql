CREATE TABLE `partitions_meta_test1` (
  `k` int(11) NOT NULL COMMENT "",
  `v` int(11) NOT NULL COMMENT ""
)engine=olap
DUPLICATE KEY(`k`)
PARTITION BY RANGE(`v`)
(PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("10")),
PARTITION p3 VALUES [("10"), ("2147483647")))
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `partitions_meta_test2` (
  `k` int(11) NOT NULL COMMENT "",
  `v` int(11) NOT NULL COMMENT ""
)engine=olap
DUPLICATE KEY(`k`)
PARTITION BY RANGE(`v`)
(PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("10")),
PARTITION p3 VALUES [("10"), ("2147483647")))
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
create table partitions_meta_test3 (k1 int, k2 int) distributed by hash(k1) buckets 1;