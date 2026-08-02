CREATE TABLE meta_schema_change (
    k1 INT,
    c0 INT,
    c_drop INT,
    c_move INT,
    v1 VARCHAR(20)
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "fast_schema_evolution" = "true",
    "base_compaction_forbidden_time_ranges" = "* * * * *"
);
INSERT INTO meta_schema_change VALUES
(1, 10, 100, 1000, 'alpha'),
(2, 20, 200, 2000, 'beta'),
(3, 30, NULL, 3000, 'gamma');
ALTER TABLE meta_schema_change RENAME COLUMN c_move TO c_renamed;
ALTER TABLE meta_schema_change DROP COLUMN c_drop;
ALTER TABLE meta_schema_change ADD COLUMN added_null INT NULL AFTER c0;
ALTER TABLE meta_schema_change ADD COLUMN added_default INT DEFAULT '7' AFTER added_null;
ALTER TABLE meta_schema_change MODIFY COLUMN c_renamed INT AFTER added_default;