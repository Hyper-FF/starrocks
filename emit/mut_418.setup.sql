CREATE TABLE `tc` (   `i` int(11) NULL COMMENT "",   `ai` array<int(11)> NULL COMMENT "",   `ass` array<varchar(100)> NULL COMMENT "",   `mi` map<int(11),int> NULL COMMENT "",   `ms` map<int(11),varchar(100)> NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(`i`) COMMENT "OLAP" DISTRIBUTED BY HASH(`i`) BUCKETS 2 PROPERTIES ( "replication_num" = "1" );
insert into tc values (4, null,null, null,null);
insert into tc values (3, null,['a','b'], null,map{1:null,null:null});
insert into tc values (1, [1,2],null, map{1:1,null:2},null);
insert into tc values (2, [1,2],['a','b'], map{1:1,null:2},map{1:'b',null:null});