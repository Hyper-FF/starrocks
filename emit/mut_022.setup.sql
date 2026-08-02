CREATE TABLE `t1` (
  `id` int(11) NULL COMMENT "",
  `array_varchar` array<varchar(100)>
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);
insert into t1 select generate_series, array_map(x -> cast(x as string), array_generate(1, generate_series % 100, 1)) from table(generate_series(1, 10000));
insert into t1 values (0, array_map(x -> CAST(x AS STRING), array_generate(1, 100000, 1))),
(10001, array_map(x -> CAST(x AS STRING), array_generate(1, 100000, 1)));