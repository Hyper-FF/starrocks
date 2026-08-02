CREATE TABLE `hj_build` (
  `k` bigint(20) NULL COMMENT "",
  `c01` bigint(20) NULL COMMENT "",
  `c02` bigint(20) NULL COMMENT "",
  `c03` bigint(20) NULL COMMENT "",
  `c04` bigint(20) NULL COMMENT "",
  `c05` bigint(20) NULL COMMENT "",
  `c06` bigint(20) NULL COMMENT "",
  `c07` bigint(20) NULL COMMENT "",
  `c08` bigint(20) NULL COMMENT "",
  `c09` bigint(20) NULL COMMENT "",
  `c10` bigint(20) NULL COMMENT "",
  `c11` bigint(20) NULL COMMENT "",
  `c12` bigint(20) NULL COMMENT "",
  `c13` bigint(20) NULL COMMENT "",
  `c14` bigint(20) NULL COMMENT "",
  `c15` bigint(20) NULL COMMENT "",
  `c16` bigint(20) NULL COMMENT "",
  `c17` bigint(20) NULL COMMENT "",
  `c18` bigint(20) NULL COMMENT "",
  `c19` bigint(20) NULL COMMENT "",
  `c20` bigint(20) NULL COMMENT "",
  `c21` bigint(20) NULL COMMENT "",
  `c22` bigint(20) NULL COMMENT "",
  `c23` bigint(20) NULL COMMENT "",
  `c24` bigint(20) NULL COMMENT "",
  `c25` bigint(20) NULL COMMENT "",
  `c26` bigint(20) NULL COMMENT "",
  `c27` bigint(20) NULL COMMENT "",
  `c28` bigint(20) NULL COMMENT "",
  `c29` bigint(20) NULL COMMENT "",
  `c30` bigint(20) NULL COMMENT "",
  `c31` bigint(20) NULL COMMENT "",
  `c32` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
CREATE TABLE `hj_probe` (
  `k` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
insert into hj_probe values (1);
insert into hj_build values ( 1, 1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1 );