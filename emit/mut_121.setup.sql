CREATE TABLE array_map_in_reuse_t (
  id INT,
  arr_datetime ARRAY<DATETIME>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);
insert into array_map_in_reuse_t values
  (1, ['2024-01-15 00:00:00', '2024-02-20 00:00:00']),
  (2, ['2024-03-10 00:00:00']),
  (3, ['2024-01-01 00:00:00']);