create table t1 (id int, data array<float>) engine = olap distributed by hash(id) properties ("replication_num" = "1");
insert into t1 values(1, array<float>[0.1, 0.2, 0.3]), (2, array<float>[0.2, 0.1, 0.3]), (3, array<float>[0.3, 0.2, 0.1]);
create table test_vector (id int, data array<float>) ENGINE=olap DUPLICATE KEY (id) DISTRIBUTED BY HASH(id) properties ("replication_num" = "1");
insert into test_vector values (1, array<float>[0.1, 0.2, 0.3]), (2, array<float>[0.2, 0.1, 0.3]);
insert into test_vector values (3, array<float>[0.15, 0.25, 0.32]), (4, array<float>[0.12, 0.11, 0.32]);
insert into test_vector values (5, array<float>[0.25, 0.12, 0.13]), (6, array<float>[0.22, 0.01, 0.39]);