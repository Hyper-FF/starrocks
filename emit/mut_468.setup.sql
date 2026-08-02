CREATE TABLE `no_partition_t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"replication_num" = "1"
);
insert into no_partition_t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  40960));
insert into no_partition_t0 select * from no_partition_t0;
CREATE TABLE `colocate_partition_t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
PARTITION BY RANGE(`c0`)
(
PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("1024")),
PARTITION p3 VALUES [("1024"), ("2048")),
PARTITION p4 VALUES [("2048"), ("4096")),
PARTITION p5 VALUES [("4096"), ("8192")),
PARTITION p6 VALUES [("8192"), ("65536")),
PARTITION p7 VALUES [("65536"), ("2100000000")))
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "colocate_t0",
"compression" = "LZ4"
);
insert into colocate_partition_t0 select * from no_partition_t0;
CREATE TABLE `no_colocate_partition_t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
PARTITION BY RANGE(`c0`)
(
PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("1024")),
PARTITION p3 VALUES [("1024"), ("2048")),
PARTITION p4 VALUES [("2048"), ("4096")),
PARTITION p5 VALUES [("4096"), ("8192")),
PARTITION p6 VALUES [("8192"), ("65536")),
PARTITION p7 VALUES [("65536"), ("2100000000")))
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "colocate_t0",
"compression" = "LZ4"
);
insert into no_colocate_partition_t0 select * from no_partition_t0;
CREATE TABLE `only_one_tablet` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into only_one_tablet select * from no_partition_t0;
CREATE TABLE `result_one_tablet_table` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
CREATE TABLE `result_table` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into result_table select distinct c0, c1, c2 from no_partition_t0;
insert into result_one_tablet_table select distinct c0, c1, c2 from no_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from no_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from no_partition_t0 limit 100;
insert into result_table select distinct c0, c1, c2 from colocate_partition_t0;
insert into result_one_tablet_table select distinct c0, c1, c2 from colocate_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from colocate_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from colocate_partition_t0 limit 100;
insert into result_table select distinct c0, c1, c2 from no_colocate_partition_t0;
insert into result_one_tablet_table select distinct c0, c1, c2 from no_colocate_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from no_colocate_partition_t0;
insert into blackhole() select distinct c0, c1, c2 from no_colocate_partition_t0 limit 100;
insert into result_table select distinct c0, c1, c2 from only_one_tablet;
insert into result_one_tablet_table select distinct c0, c1, c2 from only_one_tablet;
insert into blackhole() select distinct c0, c1, c2 from only_one_tablet;
insert into blackhole() select distinct c0, c1, c2 from only_one_tablet limit 100;