DROP TABLE if exists t0;
CREATE TABLE if not exists t0
 (
 c0 VARCHAR(255) NOT NULL,
 c1 VARCHAR(255) NOT NULL,
 c2 VARCHAR(255) NOT NULL,
 c3 VARCHAR(255) NOT NULL 
 ) ENGINE=OLAP
 DUPLICATE KEY(`c0` )
 COMMENT "OLAP"
 DISTRIBUTED BY HASH(`c0` ) BUCKETS 1
 PROPERTIES(
 "replication_num" = "1",
 "in_memory" = "false",
 "storage_format" = "default" 
 );
insert into t0
select
concat("",i,"_abc") as c0,
concat("foo_", 100000 - i) as c1,
concat("bar_", ((50000 - i)/10)*((50000 - i)/10)) as c2,
concat("", ((50000 - i)/10)*((50000 - i)/10),"_bar") as c3
from table(generate_series(1,100000)) gs(i);