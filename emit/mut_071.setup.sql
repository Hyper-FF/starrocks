CREATE TABLE t_hll_16 (
  k    int NULL,
  cnt  ds_hll_count_distinct(varchar, int, varchar) NULL
) ENGINE=OLAP
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 16
PROPERTIES ("replication_num" = "1");
CREATE TABLE t_hll_1 (
  k    int NULL,
  cnt  ds_hll_count_distinct(varchar, int, varchar) NULL
) ENGINE=OLAP
AGGREGATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 16
PROPERTIES ("replication_num" = "1");
insert into t_hll_16
select generate_series % 16, ds_hll_count_distinct_state(cast(generate_series as varchar), 17, 'HLL_6')
from table(generate_series(1, 16000));
insert into t_hll_1
select 0, ds_hll_count_distinct_state(cast(generate_series as varchar), 17, 'HLL_6')
from table(generate_series(1, 16000));