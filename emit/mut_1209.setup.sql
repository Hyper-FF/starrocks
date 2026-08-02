create table t (
    k int,
    v bigint
) duplicate key(k)
distributed by hash(k) buckets 4
properties('replication_num' = '1');
insert into t select 0, generate_series from table(generate_series(1, 40000));
insert into t select generate_series, generate_series from table(generate_series(1, 4000));
create table profile_no_skew properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);
create table profile_skew properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);