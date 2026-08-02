CREATE TABLE t (
  id bigint,
  fields json
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO t VALUES
 (1, parse_json('{"Campaign":"CapA","campaign":"lowa","title":"t1"}')),
 (2, parse_json('{"Campaign":"CapB","campaign":"lowb","title":"t2"}'));