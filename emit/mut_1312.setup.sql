CREATE TABLE `nt0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"colocate_with" = "nt0",
"replication_num" = "1"
);
insert into nt0 SELECT generate_series %4096, 4096 - generate_series, generate_series %4096 FROM TABLE(generate_series(1,  40960));
insert into nt0 select * from nt0;
create table nt1 as select * from nt0;
create table nt2 as select * from nt0;
CREATE TABLE `nt3` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"colocate_with" = "nt0",
"replication_num" = "1"
);
insert into nt3 select * from nt0;