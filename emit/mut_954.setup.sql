create table src (k int, v int) duplicate key(k)
distributed by hash(k) buckets 8 properties("replication_num" = "1");
insert into src select generate_series % 1000, generate_series from TABLE(generate_series(1, 200000));
create table dst (k int, cnt bigint, s bigint) duplicate key(k)
distributed by hash(k) buckets 8 properties("replication_num" = "1");
insert into dst select k, count(*), sum(v) from src group by k;
insert into dst
select a.k, count(*), sum(a.v)
from src a join [shuffle] (select k, count(v) v from src group by k having count(v) > 999999999) b
  on a.k = b.k
group by a.k;
insert into dst select k, count(*), sum(v) from src group by k;
drop table dst;
drop table src;