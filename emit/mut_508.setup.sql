create external table t_col_proj (c1 int, c2 string, c3 bigint, c4 string);
INSERT INTO t_col_proj VALUES (1, 'a', 100, 'b'), (2, 'c', 200, 'd'), (3, 'e', 300, 'f');
drop table t_col_proj force;