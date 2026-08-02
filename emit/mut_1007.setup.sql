create table t (id int not null, val int not null, name varchar(64))
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, name varchar(5) not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (
    dt date not null,
    val int
) duplicate key(dt)
partition by range(dt) (
    partition p2025 values less than ('2026-01-01')
)
distributed by hash(dt) buckets 1
properties("replication_num" = "1");
create table t (id int not null, val int not null, name varchar(64) not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null, name varchar(64))
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null, name varchar(64))
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null, name varchar(64))
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table src (id int not null, val varchar(16) not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
insert into src values (1,'100'),(2,'abc'),(3,'200');
create table dst (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
insert into dst select id, cast(val as int) from src;
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");
create table t (id int not null, val int not null)
    duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num" = "1");