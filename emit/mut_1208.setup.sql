create table t (
    k bigint,
    k2 int,
    v bigint,
    s varchar(64)
) duplicate key(k)
distributed by hash(k) buckets 1
properties('replication_num' = '1');
create table ds8 (
    id int
) duplicate key(id)
distributed by hash(id) buckets 1
properties('replication_num' = '1');
insert into t select generate_series, generate_series % 8, generate_series, concat('v', cast(generate_series % 100 as string)) from table(generate_series(1, 40000));
insert into t select generate_series, generate_series % 8, generate_series, concat('v', cast(generate_series % 100 as string)) from table(generate_series(40001, 80000));
insert into t select generate_series, generate_series % 8, generate_series, concat('v', cast(generate_series % 100 as string)) from table(generate_series(80001, 120000));
insert into t select generate_series, generate_series % 8, generate_series, concat('v', cast(generate_series % 100 as string)) from table(generate_series(120001, 160000));
insert into t select generate_series, generate_series % 8, generate_series, concat('v', cast(generate_series % 100 as string)) from table(generate_series(160001, 200000));
insert into ds8 select generate_series from table(generate_series(0, 7));
create table profile_scan properties("replication_num"="1") as
    select line from table(unnest(split(get_query_profile(last_query_id()), "\n"))) t(line);