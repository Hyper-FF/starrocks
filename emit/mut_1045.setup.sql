create table t1 (
    k1 int,
    k2 int,
    k3 string
)
duplicate key(k1)
distributed by hash(k1) buckets 1
properties("replication_num" = "1");
insert into t1 
select s1, s1 % 1000, repeat('a', 128) FROM TABLE(generate_series(1, 655350)) s(s1);
create table pk_t1 primary key(k1) as select * from t1;
alter table t1 compact;
alter table pk_t1 compact;
create table t1_page (k1 int) duplicate key(k1) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
insert into t1_page select s1 from table(generate_series(1, 655350)) s(s1);
alter table t1_page compact;