CREATE TABLE t_idempotent (
    c1 date NOT NULL,
    c2 varchar(20),
    c3 boolean NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(c1, c2)
PARTITION BY (c1, c3)
DISTRIBUTED BY HASH(c1)
PROPERTIES (
    "replication_num" = "1"
);
insert into t_idempotent values ("2025-12-01", "1", true), ("2025-12-01", "0", false);
insert into t_idempotent values ("2025-12-01", "1", true), ("2025-12-01", "0", false);