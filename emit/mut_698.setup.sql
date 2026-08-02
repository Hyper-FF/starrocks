CREATE TABLE `t0` (
  `c0` int(11) NULL COMMENT "",
  `c1` int(11) NULL COMMENT "",
  `c2` varchar(65533) NULL COMMENT "",
  `c3` varchar(65533) NULL COMMENT "",
  `c4` array<varchar(65533)> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`, `c1`, `c2`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);
insert into t0 select 
    i%10 as c0,
    i as c1,
    if (i % 3=0, NULL, concat("foo_",i)) as c2,
    if (i % 3=0, NULL, concat("bar_",i)) as c3,
    if (i % 3=0, NULL, [concat("foo_",i),concat("bar_",i)]) as c4
from table(generate_series(1,10000)) t(i);