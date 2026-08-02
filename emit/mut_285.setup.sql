CREATE TABLE test_bitmap_create (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_create (id) VALUES (1);
INSERT INTO test_bitmap_create (id) VALUES (2);
INSERT INTO test_bitmap_create VALUES (3, to_bitmap(100));
CREATE TABLE test_hll_create (
    id INT,
    h HLL HLL_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_hll_create (id) VALUES (1);
INSERT INTO test_hll_create (id) VALUES (2);
INSERT INTO test_hll_create VALUES (3, hll_hash(100));
CREATE TABLE test_bitmap_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_bitmap_alter ADD COLUMN bm BITMAP BITMAP_UNION DEFAULT "";
CREATE TABLE test_hll_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_hll_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_hll_alter ADD COLUMN h HLL HLL_UNION DEFAULT "";
CREATE TABLE test_bitmap_nonempty (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE test_hll_nonempty (
    id INT,
    h HLL HLL_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE test_bitmap_with_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE test_bitmap_without_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_with_default (id, name) VALUES (1, 'alice');
INSERT INTO test_bitmap_without_default (id, name) VALUES (1, 'alice');