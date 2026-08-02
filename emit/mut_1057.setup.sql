CREATE TABLE t1 (
    k1 int,
    v1 int,
    v2 varchar(32)
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select x, x, concat('aa_', x) from table(generate_series(1, 100)) t(x);
insert into t1 select x, x, concat('bb_', x) from table(generate_series(101, 200)) t(x);
insert into t1 select x, x, concat('cc_', x) from table(generate_series(201, 300)) t(x);
ALTER TABLE t1 SET ("bloom_filter_columns" = "v2");
create table profile_lines properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);