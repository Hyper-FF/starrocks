CREATE TABLE t1(
    c1 int,
    c2 bitmap
)
PRIMARY KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES(
    "replication_num"="1"
);
INSERT INTO t1 VALUES (1, to_bitmap(11)), (2, to_bitmap(22)), (3, null), (4, bitmap_empty()), (5, to_bitmap(55));