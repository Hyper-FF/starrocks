create table t(v1 bigint, v2 bigint, a1 array<string>, a2 array<string>) properties('replication_num'='1');
insert into t 
select i%10 as v1, i as v2, [concat('foo_', i%250), concat('foo_',(i+1)%250), concat('foo_',(i+2)%250)] as a1, 
[concat('bar_', (i+10)%250),concat('bar_', (i+11)%250),concat('bar_', (i+12)%250)] as a1
from table(generate_series(1, 10000)) t(i);
create table result(fingerprint bigint)properties('replication_num'='1');
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, lead(e1) over(partition by v1 order by v2, e1) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, lead(e1) over(partition by v1 order by v2, e1) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select e1, v2, lead(v1) over(partition by e1 order by v2, e1) as r
from cte0
)
select 
sum(murmur_hash3_32(e1, v2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select e1, v2, lead(v1) over(partition by e1 order by v2, e1) as r
from cte0
)
select 
sum(murmur_hash3_32(e1, v2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, e2, lag(v1) over(partition by v1 order by v2, e2) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, e2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, e2, lag(v1) over(partition by v1 order by v2, e2) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, e2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, a1, a2, e1, e2, lead(e1) over(partition by substr(e1,5) order by v2, substr(e2,5)) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, array_join(a1,","), array_join(a2,","), e1, e2, coalesce(r, ""))) as fingerprint
from cte1;
insert into result with cte0 as (
select t.v1, t.v2, t.a1, t.a2,tmp.e1, tmp.e2
from t, unnest(t.a1, t.a2) tmp(e1,e2)
),
cte1 as(
select v1, v2, a1, a2, e1, e2, lead(e1) over(partition by substr(e1,5) order by v2, substr(e2,5)) as r
from cte0
)
select 
sum(murmur_hash3_32(v1, v2, array_join(a1,","), array_join(a2,","), e1, e2, coalesce(r, ""))) as fingerprint
from cte1;