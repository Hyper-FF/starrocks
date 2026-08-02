create table t0 (c0 INT, c1 BIGINT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 4 PROPERTIES('replication_num' = '1');
insert into
    t0
SELECT
    generate_series,
    4096 - generate_series
FROM
    TABLE(generate_series(1, 4096));
insert into
    t0
select
    *
from
    t0;
insert into
    t0
select
    *
from
    t0;
create view aggregated_table as
select
    trim(c0) as c0,
    c1 as c1
from
    t0
group by
    1,
    2;