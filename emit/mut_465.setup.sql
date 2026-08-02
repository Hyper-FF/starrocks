CREATE TABLE t (txid VARCHAR(20), s1 VARCHAR(20), s2 VARCHAR(20), s3 VARCHAR(20), amt DOUBLE, mref VARCHAR(20))
  ENGINE=OLAP DUPLICATE KEY(txid) DISTRIBUTED BY HASH(txid) BUCKETS 1 PROPERTIES("replication_num"="1");
CREATE TABLE mk (mid VARCHAR(20), rate DOUBLE)
  ENGINE=OLAP DUPLICATE KEY(mid) DISTRIBUTED BY HASH(mid) BUCKETS 1 PROPERTIES("replication_num"="1");
insert into t values ('TX1','a','x','p',1000,'M1'),('TX2','b','y','q',2000,'M2'),('TX3','c','z','r',3000,'M9');
insert into mk values ('M1',10),('M2',20),('M3',30);
alter table t set ('unique_constraints'='txid');
alter table mk set ('unique_constraints'='mid');