CREATE TABLE test_lake_be_tablets (
  k1 date,
  k2 int,
  v1 int
)
PRIMARY KEY (k1, k2)
PARTITION BY RANGE(k1) (
  PARTITION p1 VALUES LESS THAN ('2025-10-01'),
  PARTITION p2 VALUES LESS THAN ('2025-11-01')
)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES('replication_num' = '1');
insert into test_lake_be_tablets values ("2025-09-01",4,100), ("2025-10-01",5,100), ("2025-10-02",4,100);
insert into test_lake_be_tablets values ("2025-10-03",5,100);