create table t1 (c1 string, c2 string);
insert into t1 values ("hello", "world"), ("smith", "blossom");
create table t1_orc properties("file_format"="orc") as select * from t1;
create table t1_textfile properties("file_format"="textfile") as select * from t1;
drop table t1 force;
drop table t1_orc force;
drop table t1_textfile force;