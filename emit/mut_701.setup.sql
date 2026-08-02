DROP TABLE IF EXISTS t_like_escape;
CREATE TABLE IF NOT EXISTS t_like_escape (
    c0 INT NOT NULL,
    c1 VARCHAR(200) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_like_escape (c0, c1) VALUES
(1, 'abc'),
(2, 'abc\\def'),
(3, 'star\\'),
(4, 'star%'),
(5, 'starrocks'),
(6, '\\asdf'),
(7, 'test\\asdf'),
(8, 'star\\test'),
(9, 'abc_def');
DROP TABLE t_like_escape;
DROP TABLE IF EXISTS t_like_cmp;
CREATE TABLE IF NOT EXISTS t_like_cmp (
    id INT NOT NULL,
    txt VARCHAR(64) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO t_like_cmp (id, txt) VALUES
(1,  'a\\b'),
(2,  '100%'),
(3,  '100abc'),
(4,  'a_b'),
(5,  'aXb'),
(6,  'abc%xyz'),
(7,  'abcXyz'),
(8,  'xyz%abc'),
(9,  'xa%by'),
(10, 'xaXby'),
(11, 'a\\xyz'),
(12, 'abc\\'),
(13, 'abcdef'),
(14, 'xyzabc'),
(15, 'xyzabcdef');
DROP TABLE t_like_cmp;