CREATE TABLE short_circuit
    (c1 int,
    c2  int)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 4
    PROPERTIES ("replication_num" = "1");
insert into short_circuit values
(1, 1),
(2, 2),
(3, 3),
(4, 4),
(5, 5),
(6, 6),
(7, 7),
(8, 8),
(9, 9),
(10, 10);
CREATE TABLE short_circuit_bool
    (k1 int,
     k2 boolean,
    c2  int)
    PRIMARY KEY(k1, k2)
    DISTRIBUTED BY HASH(k1, k2)
    BUCKETS 4
    PROPERTIES ("replication_num" = "1");
insert into short_circuit_bool values
(1, true, 1),
(2, true, 2),
(3, false, 3),
(4, true, 4),
(5, true, 5),
(6, true, 6),
(7, true, 7),
(8, true, 8),
(9, true, 9),
(10, true, 10);
CREATE TABLE t_short_circuit_part (
  `k1` date NOT NULL COMMENT "",
  `k2` int(11) NOT NULL COMMENT "",
  `k3` int(11) NULL COMMENT ""
)
PRIMARY KEY(`k1`, `k2`)
PARTITION BY RANGE(`k1`)
(PARTITION p1 VALUES [("0000-01-01"), ("2026-04-02")),
PARTITION p2 VALUES [("2026-04-02"), ("2026-04-03")))
DISTRIBUTED BY HASH(`k2`);
insert into t_short_circuit_part values("2026-04-01", 1, 1), ("2026-04-02", 2, 1);