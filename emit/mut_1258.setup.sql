CREATE TABLE t_uk (
  uk int(11) NULL,
  v1 int(11) NULL,
  v2 int(11) NULL,
  v3 int(11) NULL,
  v4 int(11) NULL,
  v5 int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(uk)
DISTRIBUTED BY HASH(uk) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE t_fk (
  id int(11) NULL,
  v1 int(11) NULL,
  v2 int(11) NULL,
  v3 int(11) NULL,
  v4 int(11) NULL,
  v5 int(11) NULL,
  fk int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
ALTER TABLE t_uk SET ("unique_constraints" = "uk");
ALTER TABLE t_fk SET ("foreign_key_constraints" = "(fk) REFERENCES t_uk(uk)");
INSERT INTO t_uk(uk, v1, v2, v3, v4, v5) VALUES
(1, 2, 3, 4, 5, 6),
(2, 3, 4, 5, 6, 7),
(3, 4, 5, 6, 7, 8),
(4, 5, 6, 7, 8, 9),
(5, 6, 7, 8, 9, 0);
INSERT INTO t_fk(id, v1, v2, v3, v4, v5, fk) VALUES
(1, 1, 1, 1, 1, 1, 1),
(2, 2, 2, 2, 2, 2, 1),
(3, 3, 3, 3, 3, 3, 1),
(4, 4, 4, 4, 4, 4, 1),
(5, 5, 5, 5, 5, 5, 1),
(6, 1, 1, 1, 1, 1, 2),
(7, 2, 2, 2, 2, 2, 2),
(8, 3, 3, 3, 3, 3, 2),
(9, 4, 4, 4, 4, 4, 2),
(10, 1, 1, 1, 1, 1, 3),
(11, 2, 2, 2, 2, 2, 3),
(12, 3, 3, 3, 3, 3, 3),
(13, 1, 1, 1, 1, 1, 4),
(14, 2, 2, 2, 2, 2, 4),
(15, 1, 1, 1, 1, 1, 5),
(16, 2, 2, 2, 2, 2, 5),
(17, 3, 3, 3, 3, 3, 5),
(18, 1, 1, 1, 1, 1, NULL),
(19, 2, 2, 2, 2, 2, NULL),
(20, 3, 3, 3, 3, 3, NULL);