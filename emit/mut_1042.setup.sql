drop table if exists predicate_lm_v1_sparse;
drop table if exists predicate_lm_v2_low_full;
drop table if exists predicate_lm_v2_low_half_null;
drop table if exists predicate_lm_v2_high_half_null;
drop table if exists predicate_lm_v2_high_full;
create table predicate_lm_v1_sparse (
    id int,
    col_string string,
    col_int int
)
duplicate key(id)
distributed by hash(id)
properties(
    "replication_num" = "1"
);
create table predicate_lm_v2_low_full (
    id int,
    col_string string not null,
    col_int int
)
duplicate key(id)
distributed by hash(id)
properties(
    "replication_num" = "1"
);
create table predicate_lm_v2_low_half_null (
    id int,
    col_string string,
    col_int int
)
duplicate key(id)
distributed by hash(id)
properties(
    "replication_num" = "1"
);
create table predicate_lm_v2_high_half_null (
    id int,
    col_string string,
    col_int int
)
duplicate key(id)
distributed by hash(id)
properties(
    "replication_num" = "1"
);
create table predicate_lm_v2_high_full (
    id int,
    col_string string not null,
    col_int int
)
duplicate key(id)
distributed by hash(id)
properties(
    "replication_num" = "1"
);
insert into predicate_lm_v1_sparse
select id,
       if(id <= 5900, NULL, if(id % 2 = 0, 'xxx', 'abc')),
       id
from table(generate_series(1, 6000)) gen(id);
insert into predicate_lm_v2_low_full
select id,
       if(id % 4 = 0, 'abc', 'xxx'),
       id
from table(generate_series(1, 8000)) gen(id);
insert into predicate_lm_v2_low_half_null
select id,
       case
           when id % 2 then NULL
           when id % 5 = 0 then 'abc'
           else 'xxx'
       end,
       id
from table(generate_series(1, 8000)) gen(id);
insert into predicate_lm_v2_high_half_null
select id,
       case
           when id % 2  then NULL
           when id % 177 then 'xxx'
           else concat('value_', id)
       end,
       id
from table(generate_series(1, 12000)) gen(id);
insert into predicate_lm_v2_high_full
select id,
       case
           when id % 177 then 'xxx'
           else concat('value_', id)
       end,
       id
from table(generate_series(1, 9000)) gen(id);
drop table if exists predicate_lm_v1_sparse;
drop table if exists predicate_lm_v2_low_full;
drop table if exists predicate_lm_v2_low_half_null;
drop table if exists predicate_lm_v2_high_half_null;
drop table if exists predicate_lm_v2_high_full;