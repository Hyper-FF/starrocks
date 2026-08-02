CREATE TABLE t1 (
  id        int(11)        NOT NULL,
  category  varchar(192)   NULL
) ENGINE=OLAP
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
  "compression"             = "LZ4",
  "enable_persistent_index" = "true",
  "fast_schema_evolution"   = "true",
  "replicated_storage"      = "true",
  "replication_num"         = "1"
);
INSERT INTO t1 (id, category)
SELECT generate_series,
       CONCAT('cat-', CAST(generate_series % 10 AS STRING))
FROM TABLE(generate_series(1, 1000));