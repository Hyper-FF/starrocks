CREATE TABLE t1 (
    k1 int,
    v1 varchar(64)
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select x, concat('apple_', x) from table(generate_series(1, 100)) t(x);
insert into t1 select x, concat('banana_', x) from table(generate_series(101, 200)) t(x);
insert into t1 select x, concat('cherry_', x) from table(generate_series(201, 300)) t(x);
ALTER TABLE t1 ADD INDEX idx_v1_ngbf (v1) USING NGRAMBF("gram_num"="3","bloom_filter_fpp"="0.05");
create table profile_lines properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);
ALTER TABLE t1 DROP INDEX idx_v1_ngbf;