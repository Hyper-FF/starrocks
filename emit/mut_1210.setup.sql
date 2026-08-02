CREATE TABLE t1 (
  k1 bigint NULL,
  c_int_1_seq bigint SUM NULL
) ENGINE=OLAP
AGGREGATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "tablet_internal_group2"
);
CREATE TABLE t2 (
  k1 bigint NULL,
  c_int_1_seq bigint SUM NULL
) ENGINE=OLAP
AGGREGATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "tablet_internal_group2"
);
insert into t1 select 1, 1;
insert into t2 select 1, 1;
insert into t2 select 1, 1;
insert into t2 select 1, 1;
insert into t2 select 1, 1;
insert into t2 select 1, 1;
insert into t2 select 1, 1;