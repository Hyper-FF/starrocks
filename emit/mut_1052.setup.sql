CREATE TABLE t1 (
    k1 int,
    v1 int,
    v2 string
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select x, 10 * (x % 6 + 1), concat('s_', x) from table(generate_series(1, 200)) t(x);
insert into t1 select x, 10 * (x % 6 + 1), concat('s_', x) from table(generate_series(201, 400)) t(x);
insert into t1 select x, 10 * (x % 6 + 1), concat('s_', x) from table(generate_series(401, 600)) t(x);
ALTER TABLE t1 ADD INDEX idx_v1(v1) USING BITMAP;
create table profile_lines properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);