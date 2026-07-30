# AST Mutation Fuzz Report (P1)

seeds: 15976, mutants: 189055, dropped as grammar-unreachable: 12656 (6.3%)

| Outcome | Count |
|---|---:|
| OK | 107732 |
| ANALYZE_REJECTED | 81119 |
| ANALYZE_INTERNAL_ERROR | 16 |
| DEPARSE_THROW | 0 |
| REPARSE_FAIL | 0 |
| REANALYZE_FAIL | 0 |
| FIXPOINT_MISMATCH | 188 |

## Bug candidates (1 distinct signatures)

### ANALYZE_INTERNAL_ERROR — ClassCastException@com.starrocks.sql.analyzer.ExpressionAnalyzer$Visitor#visitSubfieldExpr (x16)

- detail: ClassCastException: class com.starrocks.type.ScalarType cannot be cast to class com.starrocks.type.StructType (com.starrocks.type.ScalarType and com.starrocks.type.StructType are in unnamed module of loader 'app')
- seed:     SELECT 'Test12_ARRAY_ELEMENT_OPERATIONS' as test_name, id, portfolio[1].metadata as first_asset_metadata, MAP_KEYS(portfolio[1].metadata) as metadata_keys, MAP_VALUES(portfolio[1].metadata) as metadata_values FROM complex_nested_test WHERE CARDINALITY(portfolio) > 0 ORDER BY id
- mutation: SubfieldExpr[0]: `portfolio`[1] -> `srfuzz_mut_260`.`complex_nested_test`.`portfolio`[1].`price`
- mutant:   SELECT 'Test12_ARRAY_ELEMENT_OPERATIONS' AS `test_name`, `srfuzz_mut_260`.`complex_nested_test`.`id`, `srfuzz_mut_260`.`complex_nested_test`.`portfolio`[1].`metadata` AS `first_asset_metadata`, map_keys(`srfuzz_mut_260`.`complex_nested_test`.`portfolio`[1].`metadata`) AS `metadata_keys`, map_values( ...

## Non-defect signature histogram (71 signatures, 188 instances)

| count | outcome | signature |
|---:|---|---|
| 39 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` |
| 18 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID` => `ID` |
| 18 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID``ID` => `ID` |
| 7 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID` => `ID` |
| 7 | FIXPOINT_MISMATCH | fixpoint:AS ARRAY<struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID`nu => AS ARRAY<struct<star varchar(N), length int(N), numbers array<int(N)>, |
| 6 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID` => `ID` |
| 6 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID`` tinyint>' => `ID` |
| 5 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`)) NOT IN (((SELECT DB.l3.l_orderkey, concat('S', DB.l3.l_ord => `ID`.`ID`)) NOT IN ((((SELECT DB.l3.l_orderkey, concat('S', DB.l3.l_or |
| 4 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID` + N) NOT IN (((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey => `ID`.`ID` + N) NOT IN ((((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey |
| 4 | FIXPOINT_MISMATCH | fixpoint:NULL)) AS struct<`ID`a`ID` int(N), `ID`b`ID` array<int(N)>>)` => NULL)) AS struct<a int(N), b array<int(N)>>)` |
| 3 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID``ID` => `ID` FROM `ID`.`ID` |
| 3 | FIXPOINT_MISMATCH | fixpoint:'S', N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` b => 'S', N)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N]}')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID => N]}')) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:"rocks"}]]')) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID` => "rocks"}]]')) AS struct<col1 double, col2 double, col3 double>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:"a"]')) AS struct<`ID`number`ID` int(N), `ID`star`ID` varchar>)` => "a"]')) AS struct<number int(N), star varchar>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N:N:N')) AS struct<`ID`number`ID` int(N), `ID`star`ID` varchar>)` => N:N:N')) AS struct<number int(N), star varchar>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N]}]')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` array<json>>)` => N]}]')) AS struct<col1 int(N), col2 array<json>>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:'S')) AS struct<`ID`a`ID` int(N), `ID`b`ID` array<int(N)>>)` => 'S')) AS struct<a int(N), b array<int(N)>>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:TRUE)) AS struct<`ID`a`ID` int(N), `ID`b`ID` map<int(N),int(N)>>)` => TRUE)) AS struct<a int(N), b map<int(N),int(N)>>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N, N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` big => N, N)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` bigint => N)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID => N]}')) AS struct<col1 int(N), col2 int(N), col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N}')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID` => N}')) AS struct<col1 int(N), col2 int(N), col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"rocks"}]]'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID => "rocks"}]]'))) AS struct<col1 int(N), col2 int(N), col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID` => N]')) AS struct<col1 int(N), col2 int(N), col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3` => "a"]'))) AS struct<col1 int(N), col2 int(N), col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N)>)` => N]')) AS struct<col1 int(N), col2 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N)>)` => N]}'))) AS struct<col1 int(N), col2 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N)>)` => N]}')) AS struct<col1 int(N), col2 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:son(N)) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`I => son(N)) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>) |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID => N]'))) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:JSON))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`I => JSON))) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>) |
| 1 | FIXPOINT_MISMATCH | fixpoint:N}'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID => N}'))) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"rocks"}]]')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID` => "rocks"}]]')) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 in |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]'))) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID`col3` => "a"]'))) AS struct<col1 double, col2 double, col3 double>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N}'))) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID`col3`ID => N}'))) AS struct<col1 double, col2 double, col3 double>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}]'))) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID`col3` => N]}]'))) AS struct<col1 double, col2 double, col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID`` => `CAST((CAST((parse_json('S')) AS struct<col1 int(N), col2 int(N), col3 |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}'))) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID`col3`I => N]}'))) AS struct<col1 double, col2 double, col3 int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]'))) AS struct<`ID`number`ID` int(N), `ID`star`ID` varchar>)` => "a"]'))) AS struct<number int(N), star varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:son(N)) AS struct<`ID`number`ID` int(N), `ID`not_found`ID` varchar>)` => son(N)) AS struct<number int(N), not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"rocks"}]]'))) AS struct<`ID`number`ID` int(N), `ID`not_found`ID` varc => "rocks"}]]'))) AS struct<number int(N), not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]')) AS struct<`ID`number`ID` int(N), `ID`not_found`ID` varchar>)` => "a"]')) AS struct<number int(N), not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]')) AS struct<`ID`number`ID` array<int(N)>, `ID`not_found`ID` varc => "a"]')) AS struct<number array<int(N)>, not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]')) AS struct<`ID`number`ID` array<int(N)>, `ID`not_found`ID` varcha => N]')) AS struct<number array<int(N)>, not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N))) AS struct<`ID`number`ID` array<int(N)>, `ID`not_found`ID` varchar => N))) AS struct<number array<int(N)>, not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:"rocks"}]]')) AS struct<`ID`number`ID` array<int(N)>, `ID`not_found`ID => "rocks"}]]')) AS struct<number array<int(N)>, not_found varchar>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N}'))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` array<json>>)` => N}'))) AS struct<col1 int(N), col2 array<json>>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N:N:N')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` array<json>>)` => N:N:N')) AS struct<col1 int(N), col2 array<json>>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:JSON))) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` array<json>>)` => JSON))) AS struct<col1 int(N), col2 array<json>>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID`` array<int(N)>, => `CAST((parse_json(N)) AS struct<star varchar(N), length int(N), number |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID`` array<int(N)>, => `CAST((parse_json(FALSE)) AS struct<star varchar(N), length int(N), nu |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}')) AS struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID`n => N]}')) AS struct<star varchar(N), length int(N), numbers array<int(N)> |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}]')) AS struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID` => N]}]')) AS struct<star varchar(N), length int(N), numbers array<int(N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID`` array<int(N)>, => `CAST((parse_json(-N)) AS struct<star varchar(N), length int(N), numbe |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID`` array<int(N)>, => `CAST((parse_json('S')) AS struct<star varchar(N), length int(N), numb |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}'))) AS struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID` => N]}'))) AS struct<star varchar(N), length int(N), numbers array<int(N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID`` array<int(N)>, => `CAST((parse_json(TRUE)) AS struct<star varchar(N), length int(N), num |
| 1 | FIXPOINT_MISMATCH | fixpoint:"a"]')) AS struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID` => "a"]')) AS struct<star varchar(N), length int(N), numbers array<int(N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}'))) AS struct<`ID`a[`ID` int(N)>)` => N]}'))) AS struct<a[ int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:N]}')) AS struct<`ID`a.b`ID` int(N)>)` => N]}')) AS struct<a.b int(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID`` tinyint>', => `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:+ 'S') NOT IN (((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey => + 'S') NOT IN ((((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey |
| 1 | FIXPOINT_MISMATCH | fixpoint:N, N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` big => N, N)) AS struct<a double, b varchar(N), c bigint(N)>), N)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S', N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` b => 'S', N)) AS struct<a double, b varchar(N), c bigint(N)>))` |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S', N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` b => 'S', N)) AS struct<a double, b varchar(N), c bigint(N)>), 'S')`ID`DB`I |
| 1 | FIXPOINT_MISMATCH | fixpoint:N, N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` big => N, N)) AS struct<a double, b varchar(N), c bigint(N)>))`ID`DB`ID`sc2` |
| 1 | FIXPOINT_MISMATCH | fixpoint:FALSE)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` bi => FALSE)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:-N, N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` bi => -N, N)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:JSON), N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` => JSON), N)) AS struct<a double, b varchar(N), c bigint(N)>)` |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S')) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` bigi => 'S')) AS struct<a double, b varchar(N), c bigint(N)>)` |

## Non-defect outcomes (details for the 20 largest)

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID`, `ID` => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` (x39)
- mutant: SELECT `srfuzz_mut_1080`.`sc2`.`s2`, `srfuzz_mut_1080`.`sc2`.`s2`, count(DISTINCT `srfuzz_mut_1080`.`sc2`.`map1`) AS `count(DISTINCT srfuzz_mut_1080.sc2.map1)` FROM `srfuzz_mut_1080`.`sc2` GROUP BY `srfuzz_mut_1080`.`sc2`.`s2`, `s2`
- detail: s1 ...`srfuzz_mut_1080`.`sc2`.`s2`, `s2`...  |  s2 ...`srfuzz_mut_1080`.`sc2`.`s2`, `srfuzz_mut_1080`.`sc2`.`s2`...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID``ID``ID` => `ID` (x18)
- mutant: SELECT CAST((parse_json('{"a.b":7}')) AS struct<`col1` int(11), `col2` int(11)>) AS `CAST((parse_json('{"a.b":7}')) AS struct<``col1`` int(11), ``col2`` int(11)>)`
- detail: s1 ...`CAST((parse_json('{"a.b":7}')) AS struct<``col1`` int(11), ``col2`` int(11)>)`...  |  s2 ...`CAST((parse_json('{"a.b":7}')) AS struct<col1 int(11), col2 int(11)>)`...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID``ID` => `ID` (x18)
- mutant: SELECT CAST((parse_json(parse_json('{"a.b":7}'))) AS struct<`col1` int(11), `col2` int(11), `col3` int(11), `col4` int(11)>) AS `CAST((parse_json(parse_json('{"a.b":7}'))) AS struct<``col1`` int(11), ``col2`` int(11), ``col3`` int(11), ``col4`` int(11)>)`
- detail: s1 ...`CAST((parse_json(parse_json('{"a.b":7}'))) AS struct<``col1`` int(11), ``col2`` int(11), ``col3`` int(11), ``col4`` int(11)>)`...  |  s2 ...`CAST((parse_json(parse_json('{"a.b":7}'))) AS struct<col1 int(11), col2 int(11), col3 int(11), col4 int(11)>)`...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID``ID``ID``ID``ID` => `ID` (x7)
- mutant: SELECT CAST((parse_json('\\')) AS struct<`col1` int(11), `col2` int(11), `col3` int(11)>) AS `CAST((parse_json('\\')) AS struct<``col1`` int(11), ``col2`` int(11), ``col3`` int(11)>)`
- detail: s1 ...`CAST((parse_json('\\')) AS struct<``col1`` int(11), ``col2`` int(11), ``col3`` int(11)>)`...  |  s2 ...`CAST((parse_json('\\')) AS struct<col1 int(11), col2 int(11), col3 int(11)>)`...

### FIXPOINT_MISMATCH — fixpoint:AS ARRAY<struct<`ID`star`ID` varchar(N), `ID`length`ID` int(N), `ID`nu => AS ARRAY<struct<star varchar(N), length int(N), numbers array<int(N)>, (x7)
- mutant: SELECT CAST((parse_json(parse_json('[{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}, {"star" : "rockses", "length": 33, "numbers": [2, 5, 9], "nest": [3, 6, 9]}]'))) AS ARRAY<struct<`star` varchar(10), `length` int(11), `numbers` array<int(11)>, `nest` struct<`col1` int(11) ...
- detail: s1 ...AS ARRAY<struct<``star`` varchar(10), ``length`` int(11), ``numbers`` array<int(11)>,...  |  s2 ...AS ARRAY<struct<star varchar(10), length int(11), numbers array<int(11)>, nest struct<col1...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID` => `ID` (x6)
- mutant: SELECT CAST((parse_json(parse_json('[1,2,3]'))) AS struct<`a[` int(11)>) AS `CAST((parse_json(parse_json('[1,2,3]'))) AS struct<``a[`` int(11)>)`
- detail: s1 ...`CAST((parse_json(parse_json('[1,2,3]'))) AS struct<``a[`` int(11)>)`...  |  s2 ...`CAST((parse_json(parse_json('[1,2,3]'))) AS struct<a[ int(11)>)`...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID`` tinyint>' => `ID` (x6)
- mutant: SELECT 'smallint' AS `typeof(CAST('struct<``col1`` tinyint, ``col2`` tinyint, ``col3`` tinyint, ``col4`` tinyint>' AS SMALLINT))`
- detail: s1 ...`typeof(CAST('struct<``col1`` tinyint, ``col2`` tinyint, ``col3`` tinyint, ``col4`` tinyint>'...  |  s2 ...`typeof(CAST('struct<col1 tinyint, col2 tinyint, col3 tinyint, col4 tinyint>' AS SMALLINT))`...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`)) NOT IN (((SELECT DB.l3.l_orderkey, concat('S', DB.l3.l_ord => `ID`.`ID`)) NOT IN ((((SELECT DB.l3.l_orderkey, concat('S', DB.l3.l_or (x5)
- mutant: SELECT count(*) AS `count(*)` FROM (SELECT `t`.`x0` FROM (SELECT if(`table_function_generate_series`.`generate_series` <= 1000, NULL, `table_function_generate_series`.`generate_series`) AS `x0` FROM TABLE(generate_series(1,8192,1))) `t` WHERE (`t`.`x0`, concat('l', `t`.`x0`)) NOT IN (((SELECT srfuzz ...
- detail: s1 ...`t`.`x0`)) NOT IN (((SELECT srfuzz_mut_669.l3.l_orderkey, concat('l', srfuzz_mut_669.l3.l_orderkey)...  |  s2 ...`t`.`x0`)) NOT IN ((((SELECT srfuzz_mut_669.l3.l_orderkey, concat('l', srfuzz_mut_669.l3.l_orderkey)...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID` + N) NOT IN (((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey => `ID`.`ID` + N) NOT IN ((((SELECT DB.l3.l_orderkey, DB.l3.l_orderkey (x4)
- mutant: SELECT count(*) AS `count(*)` FROM (SELECT `t`.`x0` FROM (SELECT if(5 NOT IN ((SELECT `srfuzz_mut_669`.`r`.`k2` FROM `srfuzz_mut_669`.`build1` AS `r`)), NULL, `table_function_generate_series`.`generate_series`) AS `x0` FROM TABLE(generate_series(1,8192,1))) `t` WHERE (`t`.`x0`, `t`.`x0` + 1) NOT IN  ...
- detail: s1 ...`t`.`x0` + 1) NOT IN (((SELECT srfuzz_mut_669.l3.l_orderkey, srfuzz_mut_669.l3.l_orderkey...  |  s2 ...`t`.`x0` + 1) NOT IN ((((SELECT srfuzz_mut_669.l3.l_orderkey, srfuzz_mut_669.l3.l_orderkey...

### FIXPOINT_MISMATCH — fixpoint:NULL)) AS struct<`ID`a`ID` int(N), `ID`b`ID` array<int(N)>>)` => NULL)) AS struct<a int(N), b array<int(N)>>)` (x4)
- mutant: SELECT CAST((json_query(1, NULL)) AS struct<`a` int(11), `b` array<int(11)>>) AS `CAST((json_query(1, NULL)) AS struct<``a`` int(11), ``b`` array<int(11)>>)`
- detail: s1 ...NULL)) AS struct<``a`` int(11), ``b`` array<int(11)>>)`...  |  s2 ...NULL)) AS struct<a int(11), b array<int(11)>>)`...

### FIXPOINT_MISMATCH — fixpoint:`ID``ID``ID``ID``ID``ID``ID``ID``ID` => `ID` FROM `ID`.`ID` (x3)
- mutant: SELECT 'varchar' AS `typeof('struct<``col1`` tinyint, ``col2`` tinyint, ``col3`` tinyint, ``col4`` tinyint>')` FROM `srfuzz_mut_448`.`test_uv`
- detail: s1 ...`typeof('struct<``col1`` tinyint, ``col2`` tinyint, ``col3`` tinyint, ``col4`` tinyint>')`...  |  s2 ...`typeof('struct<col1 tinyint, col2 tinyint, col3 tinyint, col4 tinyint>')` FROM `srfuzz_mut_448`.`test_uv`...

### FIXPOINT_MISMATCH — fixpoint:'S', N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` b => 'S', N)) AS struct<a double, b varchar(N), c bigint(N)>)` (x3)
- mutant: SELECT CAST((row(1, '1', 3)) AS struct<`a` double, `b` varchar(65533), `c` bigint(20)>) AS `CAST((row(1, '1', 3)) AS struct<``a`` double, ``b`` varchar(65533), ``c`` bigint(20)>)`
- detail: s1 ...'1', 3)) AS struct<``a`` double, ``b`` varchar(65533), ``c`` bigint(20)>)`...  |  s2 ...'1', 3)) AS struct<a double, b varchar(65533), c bigint(20)>)`...

### FIXPOINT_MISMATCH — fixpoint:N]}')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` int(N), `ID`col3`ID => N]}')) AS struct<col1 int(N), col2 int(N), col3 int(N), col4 int(N)>)` (x2)
- mutant: SELECT CAST((parse_json('{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}')) AS struct<`col1` int(11), `col2` int(11), `col3` int(11), `col4` int(11)>) AS `CAST((parse_json('{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}')) AS struct<``col1`` int(11) ...
- detail: s1 ...3]}')) AS struct<``col1`` int(11), ``col2`` int(11), ``col3`` int(11), ``col4`` int(11)>)`...  |  s2 ...3]}')) AS struct<col1 int(11), col2 int(11), col3 int(11), col4 int(11)>)`...

### FIXPOINT_MISMATCH — fixpoint:"rocks"}]]')) AS struct<`ID`col1`ID` double, `ID`col2`ID` double, `ID` => "rocks"}]]')) AS struct<col1 double, col2 double, col3 double>)` (x2)
- mutant: SELECT CAST((parse_json('[1, [{"star": "rocks"}, {"star": "rocks"}]]')) AS struct<`col1` double, `col2` double, `col3` double>) AS `CAST((parse_json('[1, [{"star": "rocks"}, {"star": "rocks"}]]')) AS struct<``col1`` double, ``col2`` double, ``col3`` double>)`
- detail: s1 ..."rocks"}]]')) AS struct<``col1`` double, ``col2`` double, ``col3`` double>)`...  |  s2 ..."rocks"}]]')) AS struct<col1 double, col2 double, col3 double>)`...

### FIXPOINT_MISMATCH — fixpoint:"a"]')) AS struct<`ID`number`ID` int(N), `ID`star`ID` varchar>)` => "a"]')) AS struct<number int(N), star varchar>)` (x2)
- mutant: SELECT CAST((parse_json('[1, 2, 3, "a"]')) AS struct<`number` int(11), `star` varchar>) AS `CAST((parse_json('[1, 2, 3, "a"]')) AS struct<``number`` int(11), ``star`` varchar>)`
- detail: s1 ..."a"]')) AS struct<``number`` int(11), ``star`` varchar>)`...  |  s2 ..."a"]')) AS struct<number int(11), star varchar>)`...

### FIXPOINT_MISMATCH — fixpoint:N:N:N')) AS struct<`ID`number`ID` int(N), `ID`star`ID` varchar>)` => N:N:N')) AS struct<number int(N), star varchar>)` (x2)
- mutant: SELECT CAST((parse_json('9999-12-31 23:59:59')) AS struct<`number` int(11), `star` varchar>) AS `CAST((parse_json('9999-12-31 23:59:59')) AS struct<``number`` int(11), ``star`` varchar>)`
- detail: s1 ...23:59:59')) AS struct<``number`` int(11), ``star`` varchar>)`...  |  s2 ...23:59:59')) AS struct<number int(11), star varchar>)`...

### FIXPOINT_MISMATCH — fixpoint:N]}]')) AS struct<`ID`col1`ID` int(N), `ID`col2`ID` array<json>>)` => N]}]')) AS struct<col1 int(N), col2 array<json>>)` (x2)
- mutant: SELECT CAST((parse_json('[{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}, {"star" : "rockses", "length": 33, "numbers": [2, 5, 9], "nest": [3, 6, 9]}]')) AS struct<`col1` int(11), `col2` array<json>>) AS `CAST((parse_json('[{"star" : "rocks", "length": 5, "numbers": [1, 4,  ...
- detail: s1 ...9]}]')) AS struct<``col1`` int(11), ``col2`` array<json>>)`...  |  s2 ...9]}]')) AS struct<col1 int(11), col2 array<json>>)`...

### FIXPOINT_MISMATCH — fixpoint:'S')) AS struct<`ID`a`ID` int(N), `ID`b`ID` array<int(N)>>)` => 'S')) AS struct<a int(N), b array<int(N)>>)` (x2)
- mutant: SELECT CAST((row(1, 's1')) AS struct<`a` int(11), `b` array<int(11)>>) AS `CAST((row(1, 's1')) AS struct<``a`` int(11), ``b`` array<int(11)>>)`
- detail: s1 ...'s1')) AS struct<``a`` int(11), ``b`` array<int(11)>>)`...  |  s2 ...'s1')) AS struct<a int(11), b array<int(11)>>)`...

### FIXPOINT_MISMATCH — fixpoint:TRUE)) AS struct<`ID`a`ID` int(N), `ID`b`ID` map<int(N),int(N)>>)` => TRUE)) AS struct<a int(N), b map<int(N),int(N)>>)` (x2)
- mutant: SELECT CAST((json_query(1, TRUE)) AS struct<`a` int(11), `b` map<int(11),int(11)>>) AS `CAST((json_query(1, TRUE)) AS struct<``a`` int(11), ``b`` map<int(11),int(11)>>)`
- detail: s1 ...TRUE)) AS struct<``a`` int(11), ``b`` map<int(11),int(11)>>)`...  |  s2 ...TRUE)) AS struct<a int(11), b map<int(11),int(11)>>)`...

### FIXPOINT_MISMATCH — fixpoint:N, N)) AS struct<`ID`a`ID` double, `ID`b`ID` varchar(N), `ID`c`ID` big => N, N)) AS struct<a double, b varchar(N), c bigint(N)>)` (x2)
- mutant: SELECT CAST((row(repeat('x', 65536), 2, 3)) AS struct<`a` double, `b` varchar(65533), `c` bigint(20)>) AS `CAST((row(repeat('x', 65536), 2, 3)) AS struct<``a`` double, ``b`` varchar(65533), ``c`` bigint(20)>)`
- detail: s1 ...2, 3)) AS struct<``a`` double, ``b`` varchar(65533), ``c`` bigint(20)>)`...  |  s2 ...2, 3)) AS struct<a double, b varchar(65533), c bigint(20)>)`...
