CREATE TABLE `pre_agg_case` (
  `c0` int(11) NULL COMMENT "",
  `c1` char(50) NULL COMMENT "",
  `c2` int(11) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 5
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
insert into pre_agg_case select generate_series, generate_series, generate_series, generate_series from TABLE(generate_series(1, 50));
CREATE TABLE __row_util_base (
  k1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util_base select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
insert into __row_util_base select * from __row_util_base;
CREATE TABLE __row_util (
  idx bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
insert into __row_util select row_number() over() as idx from __row_util_base;
CREATE TABLE t1 (
  k1 bigint NULL,
  c1 bigint NULL,
  c2 int NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 select idx, idx % (20480000 / 26), idx from __row_util;