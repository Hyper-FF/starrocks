create table t1 (
    k1 int NULL,
    date_str string,
    datetime_str string,
    date_str_with_whitespace string,
    datetime_str_with_whitespace string
)
duplicate key(k1)
distributed by hash(k1) buckets 32;
CREATE TABLE __row_util (
  k1 bigint null
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32;
insert into __row_util select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into __row_util select k1 + 20000 from __row_util;
insert into __row_util select k1 + 40000 from __row_util;
insert into __row_util select k1 + 80000 from __row_util;
insert into t1
select
    cast(random() *2e9 as int),
    cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar),
    concat(cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), ' 01:02:03'),
    concat('  ', cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), '  '),
    concat('   ', cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), ' 01:02:03  ')
from __row_util;
insert into t1 (date_str)
values
    ("20200101"),
    ("20200101010203"),
    ("20200101T010203"),
    ("20200101T0102033"),
    ("20200101T010203.123"),
    ("200101"),
    ("200101010203"),
    ("200101T010203"),
    ("200101T0102033"),
    ("200101T010203.123"),
    ("2020.01.01"),
    ("2020.01.01T01.02.03"),
    ("2020.01.01T01.02.033"),
    ("2020.01.01T01.0203.123"),
    ("  20200101 "),
    ("  20200101010203   "),
    ("  20200101T010203"),
    ("20200101T0102033  "),
    ("  20200101T010203.123    \n"),
    ("  20200101T010203.123    \n \t \v \f \r "),
    ("2020.13.29"),
    ("2020.13.61"),
    ("2020.02.29"),
    ("2020.02.28"),
    ("2000.02.29"),
    ("2000.02.28"),
    ("2021.02.28"),
    ("2100.02.28"),
    ("invalid"),
    ("2021.02.29"),
    ("2100.02.29"),
    ("?100.02.28"),
    ("2?00.02.28"),
    ("21?0.02.28"),
    ("210?.02.28"),
    ("2100902.28"),
    ("2100.?2.28"),
    ("2100.0?.28"),
    ("2100.02928"),
    ("2100.02.?8"),
    ("2100.02.2?"),
    ("2020-01-01T01:02:03Z"),            
    ("2020-01-01T01:02:03.123Z"),        
    ("2020-01-01T01:02:03.123456Z"),     
    ("2020-01-01T01:02:03.1Z"),          
    ("  2020-01-01T01:02:03Z  "),        
    ("  2020-01-01T01:02:03.123Z  "),    
    ("2020-01-01T01:02:03z"),            
    ("2020-01-01T01:02:03Zextra"),       
    ("2020-01-01T01:02:03.Z"),           
    ("2020-01-01T01:02:03.1234567Z");
insert into t1
select
    cast(random() *2e9 as int),
    null, null, null ,null
from __row_util;