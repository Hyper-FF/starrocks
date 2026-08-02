CREATE TABLE `t0` (
  `v1` int(11) NOT NULL,
  `v2` int(11) NOT NULL,
  `v3` int(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
INSERT INTO `t0` (v1, v2, v3) values
    (1, 1, 1),
    (1, 1, 2),
    (1, 1, 3),
    (1, 2, 4),
    (1, 2, 5),
    (1, 2, 6),
    (2, 3, 7),
    (2, 3, 8),
    (2, 3, 9),
    (2, 4, 10),
    (2, 4, 11),
    (2, 4, 12);
CREATE TABLE `temp1` (
  `v1` int(11) NOT NULL,
  `v2` int(11) NOT NULL,
  `v3` int(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
INSERT INTO `temp1` (v1, v2, v3) values
    (1, 1, 1),
    (1, 1, 2),
    (1, 1, 3),
    (1, 2, 4),
    (1, 2, 5),
    (1, 2, 6),
    (2, 3, 7),
    (2, 3, 8),
    (2, 3, 9),
    (2, 4, 10),
    (2, 4, 11),
    (2, 4, 12);
create view profile_filter_stmt as
select trim(unnest) 
from table(unnest(split(get_query_profile(last_query_id()), '\n')))
where unnest like '%Sql Statement%'
order by unnest;