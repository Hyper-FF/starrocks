create table dup_t (
    k1 int,
    k2 int,
    c1 string
)
duplicate key(k1, k2)
distributed by hash(k1) buckets 3
properties("replication_num" = "1");
insert into dup_t
select generate_series, generate_series + 10000, concat('a', generate_series) from TABLE(generate_series(0, 10000 - 1));
create table uniq_t (
    k1 int,
    k2 int,
    c1 string
)
unique key(k1, k2)
distributed by hash(k1) buckets 3
properties("replication_num" = "1");
insert into uniq_t select * from dup_t;