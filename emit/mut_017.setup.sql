create table base_table (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');
insert into base_table SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  10000));
create table agg_with_limit_seq (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into agg_with_limit_seq SELECT * FROM base_table;