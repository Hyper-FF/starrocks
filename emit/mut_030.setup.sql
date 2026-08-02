CREATE TABLE `t1` (
  `tinyint_col_1` tinyint NOT NULL,
  `tinyint_col_2` tinyint
) ENGINE=OLAP
PROPERTIES (
"replication_num" = "1"
);
insert into t1 values (1, 1), (1, 2), (1,3), (1,4), (2, null), (3, null), (4, null);