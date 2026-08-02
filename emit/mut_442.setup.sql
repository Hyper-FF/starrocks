create table t1 (id int, v bigint) distributed by hash(id) properties ("replication_num" = "1");
insert into t1 values(1, 9223372036854775807), (2, -9223372036854775807), (3, 9223372036854775806);