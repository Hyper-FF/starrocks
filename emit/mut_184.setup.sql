CREATE TABLE `t1` (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 select 1, bitmap_empty();
insert into t1 select 2, to_bitmap(2);
insert into t1 select 3, bitmap_agg(generate_series) from table(generate_series(3, 12280));
insert into t1 select 4, bitmap_agg(generate_series) from table(generate_series(4, 20));
insert into t1 select 5, null;
CREATE TABLE test_tags (
	c1 varchar(65533) NOT NULL,
	tag_name varchar(65533) NOT NULL,
	tag_value varchar(65533) NOT NULL,
	rb bitmap NOT NULL
) ENGINE=OLAP
    PRIMARY KEY(c1, tag_name, tag_value)
PARTITION BY (c1)
DISTRIBUTED BY HASH(tag_name, tag_value)
PROPERTIES (
"replication_num" = "1"
);
insert into test_tags(c1, tag_name, tag_value, rb) SELECT '20250114050000','a','57',bitmap_from_string("57");
insert into test_tags(c1, tag_name, tag_value, rb) SELECT '20250114050000','a','a',bitmap_from_string("57,22253296,29101576,43027104");