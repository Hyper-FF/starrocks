CREATE TABLE `t0` (
  `c0` int(11) NULL,
  `c1` struct<a int(11), b int(11)> NULL
) 
DUPLICATE KEY(`c0`);
INSERT INTO t0 VALUES (1, NULL);
INSERT INTO t0 VALUES (2, row(1, 1));