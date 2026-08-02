CREATE TABLE `sc2` (
  `v1` bigint(20) NULL COMMENT "",
  `map1` MAP<int(11),MAP<int(11),MAP<int(11),int(11)>>> NULL COMMENT "",
  `st1` STRUCT<s1 int(11), sm2 MAP<int(11),int(11)>, sm3 MAP<int(11),MAP<int(11),int(11)>>> NULL COMMENT "",
  `st2` STRUCT<s1 int(11), sa2 ARRAY<int(11)>, ss3 STRUCT<sss1 int(11), sss2 STRUCT<ssss1 int(11), ssss2 int(11)>>> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
CREATE TABLE `t1` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "false",
"compression" = "LZ4"
);
insert into t1 values (1, 2, 3);
insert into t1 values (2, 2, 2);
insert into t1 values (3, 3, 3);
insert into t1 values (4, 4, 4);
insert into t1 values (5, 5, 5);
insert into sc2 values (1, map{1: map{10:map{100: 101}}}, row(1, map{10: 101}, map{10: map{100: 101}}), row(1, [1,10,101], row(1, row(10, 11))));
insert into sc2 values (2, map{2: map{20:map{200: 202}}}, row(2, map{20: 202}, map{20: map{200: 202}}), row(2, [2,20,202], row(2, row(20, 22))));
insert into sc2 values (3, map{3: map{30:map{300: 303}}}, row(3, map{30: 303}, map{30: map{300: 303}}), row(3, [3,30,303], row(3, row(30, 33))));
insert into sc2 values (4, map{4: map{40:map{400: 404}}}, row(4, map{40: 404}, map{40: map{400: 404}}), row(4, [4,40,404], row(4, row(40, 44))));
insert into sc2 values (5, map{5: map{50:map{500: 505}}}, row(5, map{50: 505}, map{50: map{500: 505}}), row(5, [5,50,505], row(5, row(50, 55))));
insert into sc2 values (5, map{5: map{50:map{500: 506}}}, row(5, map{50: 506}, map{50: map{500: 506}}), row(5, [5,50,506], row(5, row(50, 56))));
insert into sc2 values (5, map{5: map{50:map{500: 507}}}, row(5, map{50: 507}, map{50: map{500: 507}}), row(5, [5,50,507], row(5, row(50, 57))));
insert into sc2 values (5, map{5: map{50:map{500: 508}}}, row(5, map{50: 508}, map{50: map{500: 508}}), row(5, [5,50,508], row(5, row(50, 58))));