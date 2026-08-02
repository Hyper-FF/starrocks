create table test (pk bigint NOT NULL, v0 string not null default 'defaultv0', v1 int not null default '100001')
primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES("replication_num" = "1");
insert into test values(1, 'v0', 1), (2, 'v2', 2);
insert into test values(2, 'v2_2', default);
insert into test values(1, 'v0', 1), (2, 'v2', 2);
insert into test (pk, v1) values(1, 11);
insert into test values(1, 'v0', 1), (2, 'v2', 2);
insert into test (pk, v1) values(1, 111);
create table test2 (pk bigint NOT NULL, v0 string not null, v1 int not null default '100001')
primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES("replication_num" = "1");
insert into test2 values(1, 'v0', 1), (2, 'v2', 2);
insert into test2 (pk, v1) values(1, 11), (3, 3);
insert into test2 values(1, 'v0', 1), (2, 'v2', 2);
insert into test2 (pk, v1) values(1, 11), (3, 3);
create table test3 (pk bigint NOT NULL, v0 string not null, v1 int not null default '100001', v2 int as cast(v1 + 1 as int))
primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES("replication_num" = "1");
insert into test3 values(1, 'v0', 1), (2, 'v2', 2);
insert into test3 (pk, v1) values(1, 11), (3, 3);
create table test4 (pk bigint NOT NULL, v0 string not null, v1 int not null default '0')
primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 1 order by (v1) PROPERTIES("replication_num" = "1");
insert into test4 values(1, 'v0', 1), (2, 'v2', 2);
insert into test4 (pk, v0) values(1, 'v0_1'), (3, 'v3_1');
CREATE TABLE `t_non_replicated_storage_multi_replica_pk` (
  `k` STRING NOT NULL COMMENT "",
  `v1` int DEFAULT "10",
  `v2` int
) ENGINE=OLAP 
PRIMARY KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replication_num" = "2",
"replicated_storage" = "false"
);
insert into t_non_replicated_storage_multi_replica_pk (k,v2) select "abc", 10;
CREATE TABLE t_gin (
    pk BIGINT NOT NULL,
    v1 INT NOT NULL DEFAULT '0',
    title VARCHAR(255) NOT NULL DEFAULT '',
    INDEX gin_title (title) USING GIN ("parser" = "english")
)
PRIMARY KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1
PROPERTIES ("replicated_storage" = "true", "replication_num" = "1");
insert into t_gin values (1, 100, 'hello world'), (2, 200, 'starrocks rocks');
insert into t_gin (pk, v1) values (1, 111), (3, 300);