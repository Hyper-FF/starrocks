CREATE TABLE t0 (
    c1 int,
    c2 int
) UNIQUE KEY(c1, c2)
PARTITION BY RANGE (c1) (
    PARTITION p1 VALUES [('1'), ('10')),
    PARTITION p2 VALUES [('10'), ('20'))
) DISTRIBUTED BY HASH(c2) buckets 12
PROPERTIES( "replication_num"="1", "colocate_with"="5a5fd327dsdb_2806" );
insert into t0 (c1, c2) values (1, 1), (2, 1), (3, 1), (4, 1), (11, 1), (11, 2), (1, 2), (2, 2), (3, 2), (11, 2), (12, 2), (1, 3), (2, 3), (3, 3), (11, 3);
CREATE TABLE t1 (
    c1 int,
    c2 int
) DUPLICATE KEY(c1, c2)
PARTITION BY RANGE (c1) (
    PARTITION p1 VALUES [('1'), ('10')),
    PARTITION p2 VALUES [('10'), ('20'))
) DISTRIBUTED BY HASH(c2) buckets 12
PROPERTIES( "replication_num"="1", "colocate_with"="5a5fd327dsdb_2806" );
insert into t1 (c1, c2) values (1, 1), (2, 1), (3, 1), (4, 1), (11, 1), (11, 2), (1, 2), (2, 2), (3, 2), (11, 2), (12, 2), (1, 3), (2, 3), (3, 3), (11, 3);
CREATE TABLE t2 (
    c1 int,
    c2 int
) DUPLICATE KEY(c1, c2)
PARTITION BY RANGE (c1,c2) (
    PARTITION p1 VALUES [('1'), ('10')),
    PARTITION p2 VALUES [('10'), ('20'))
) DISTRIBUTED BY HASH(c2) buckets 12
PROPERTIES( "replication_num"="1", "colocate_with"="5a5fd327dsdb_2806" );
insert into t2 (c1, c2)values (1, 1), (2, 1), (3, 1), (4, 1), (11, 1), (11, 2), (1, 2), (2, 2), (3, 2), (11, 2), (12, 2), (1, 3), (2, 3), (3, 3), (11, 3);
CREATE TABLE t3 (
    c1 int,
    c2 int
) DUPLICATE KEY(c1, c2)
PARTITION BY RANGE (c2) (
    PARTITION p1 VALUES [('1'), ('10')),
    PARTITION p2 VALUES [('10'), ('20'))
) DISTRIBUTED BY HASH(c2) buckets 12
PROPERTIES( "replication_num"="1", "colocate_with"="5a5fd327dsdb_2806" );
insert into t3 (c1, c2)values (1, 1), (2, 1), (3, 1), (4, 1), (11, 1), (11, 2), (1, 2), (2, 2), (3, 2), (11, 2), (12, 2), (1, 3), (2, 3), (3, 3), (11, 3),(1,11);
CREATE TABLE t4 (
    c1 int, c2 int
) DUPLICATE KEY(c1, c2)
DISTRIBUTED BY HASH(c2) buckets 12
PROPERTIES( "replication_num"="1", "colocate_with"="5a5fd327dsdb_2806" );
insert into t4 (c1, c2)values (1, 1), (2, 1), (3, 1), (4, 1), (11, 1), (11, 2), (1, 2), (2, 2), (3, 2), (11, 2), (12, 2), (1, 3), (2, 3), (3, 3), (11, 3);