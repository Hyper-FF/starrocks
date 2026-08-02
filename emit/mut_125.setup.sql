create table t0 (
    k0 int,
    c0 array<tinyint>,
    c1 array<smallint>,
    c2 array<int>,
    c3 array<bigint>,
    c4 array<largeint>,
    c5 array<double>,
    c6 array<float>
)
properties(
   "replication_num" = "1"
);
insert into t0 select i
       ,[3,2,5,1,2] as c0
       ,[3,2,5,1,2] as c1
       ,[3,2,5,1,2] as c2
       ,[3,2,5,1,2] as c3
       ,[3,2,5,1,2] as c4
       ,[3,2,5,1,2] as c5
       ,[3,2,5,1,2] as c6
from table(generate_series(0,10000)) t(i);
drop table t0;
create table t0 (
    k0 int,
    c0 array<string>,
    c1 array<string>
)
properties(
   "replication_num" = "1"
);
insert into t0 select i
       ,['bc','ab','dc'] as c0
       ,['a','abcd','abc'] as c1
from table(generate_series(0,10000)) t(i);
drop table t0;
create table t0 (
    k0 int,
    c0 array<int>
)
properties(
   "replication_num" = "1"
);
insert into t0 select i
       ,[3,2,null,5,null,1,2] as c0
from table(generate_series(0,10000)) t(i);
drop table t0;
create table t0 (
    k0 int,
    c0 array<array<int>>
)
properties(
   "replication_num" = "1"
);
insert into t0 select i
      ,[[2, 3, 1], [4, 2, 1, 4], [1, 2]] c0
from table(generate_series(0,10000)) t(i);
drop table t0;