CREATE TABLE `test_dict_merge` (
  `id` int NULL COMMENT "",
  `city` string NOT NULL COMMENT "",
  `city_null` string NULL COMMENT "",
  `city_array` array<string> NOT NULL COMMENT "",
  `city_array_null` array<string> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
insert into test_dict_merge values
(1, "beijing", "beijing", ["beijing", "shanghai"], NULL),
(1, "beijing", NULL, ["shenzhen", "shanghai"], ["shenzhen", "shanghai"]),
(1, "shanghai", "shanghai", ["shenzhen", NULL], ["shenzhen", NULL]),
(1, "shanghai", NULL, ["beijing", NULL, "shanghai"], NULL);
CREATE TABLE t1 (
    c1 int,
    c2 string
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select generate_series, cast(generate_series as int) from table(generate_series(1, 1000));
CREATE TABLE t2 (
    c1 int,
    c2 string
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t2 select generate_series %5, cast(generate_series as int) %5 from table(generate_series(1, 1000));