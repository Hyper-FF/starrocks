# AST Mutation Fuzz Report (P1)

seeds: 16319, mutants: 562008, dropped as grammar-unreachable: 31923 (5.4%)

## Dropped mutants (2015 distinct reasons)

| count | reason | sample |
|---:|---|---|
| 23939 | deparse-threw:NullPointerException@com.starrocks.sql.analyzer.AnalyzerUtils#replaceNullType2Boolean | `<unrenderable>` |
| 338 | reparse-failed:ParsingException: Getting syntax error at line 3, column 37. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT approx_top_k(`c_int`) FROM `t_with_null` WHERE `c_id` > (PseudoType.AnyMapType{})` |
| 318 | reparse-failed:ParsingException: Getting syntax error at line 1, column 40. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT `map1` <=> (PseudoType.AnyMapType{14:41,NULL:11,12:31}) FROM `sc2`` |
| 311 | reparse-failed:ParsingException: Getting syntax error at line 1, column 12. Detail message: Unexpected input '(', the most similar input is {<EOF>, ';'}. | `SELECT count(1.`c2`) FROM `t0` [_META_]` |
| 308 | reparse-failed:ParsingException: Getting syntax error at line 2, column 34. Detail message: Unexpected input '{', the most similar input is {')'}. | `SELECT DISTINCT sum_map(`m`)[1], sum_map(`m`)[2], sum_map(`m`)[3] FROM (SELECT PseudoType.AnyMapType{1:10,2:20} AS `m` UNION ALL SELECT NULL AS `m` UNION ALL SELECT PseudoType.AnyMapType{1:5,3:30} AS `m`) `t`` |
| 304 | reparse-failed:ParsingException: Getting syntax error at line 1, column 38. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT `map1` = (PseudoType.AnyMapType{14:NULL,NULL:11,12:31}) FROM `sc2` ORDER BY `v1` ASC` |
| 281 | reparse-failed:ParsingException: Getting syntax error at line 1, column 28. Detail message: Unexpected input '{', the most similar input is {<EOF>, ';'}. | `SELECT PseudoType.AnyMapType{MAP<BOOLEAN,TINYINT>{NULL:1}:1}` |
| 260 | reparse-failed:ParsingException: Getting syntax error at line 1, column 39. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT CASE WHEN (PseudoType.AnyMapType{}) THEN NULL WHEN 0 THEN NULL WHEN 1 THEN NULL ELSE NULL END AS `cc` FROM `t` ORDER BY 1 ASC` |
| 238 | reparse-failed:ParsingException: Getting syntax error at line 1, column 29. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT (PseudoType.AnyMapType{}) - (CAST(3 AS INT))` |
| 120 | reparse-failed:ParsingException: Getting syntax error at line 1, column 62. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT round(covar_samp(`val1`, `val2`), PseudoType.AnyMapType{}) FROM `t1_nonnull`` |
| 95 | reparse-failed:ParsingException: Getting syntax error at line 1, column 34. Detail message: No viable statement for input 'count(PseudoType.AnyMapType{'. | `SELECT count(PseudoType.AnyMapType{}) FROM `test_pk_tbl1`` |
| 86 | reparse-failed:ParsingException: Getting syntax error at line 1, column 52. Detail message: No viable statement for input '((CAST((CAST((PseudoType.AnyMapType{'. | `SELECT CASE WHEN ((CAST((CAST((PseudoType.AnyMapType{1:row(ARRAY<MAP<VARCHAR,INT>>[PseudoType.AnyMapType{'a':1}, PseudoType.AnyMapType{'b':2}])}) AS VARIANT)) AS VARCHAR)) = '{"1":{"col1":[{"a":1},{"b":2}]}}') THEN 'PASS' ELSE 'FAIL' END AS `v_map_struct_array_map` WHERE (CAST((CAST((ARRAY<struct<`col1` int(11), `col2` varchar>>[row(1, 'a'), row(2, 'b')]) AS VARIANT)) AS VARCHAR)) = '[{"col1":1,"c ...` |
| 84 | reparse-failed:ParsingException: Getting syntax error at line 1, column 19. Detail message: Unexpected input '"age"', the most similar input is {a legal identifier}. | `SELECT * EXCLUDE ( "age" )  FROM (SELECT * FROM `test_table`) `test_table`` |
| 78 | reparse-failed:ParsingException: Getting syntax error at line 1, column 45. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT dict_merge(`c2`, PseudoType.AnyMapType{}) FROM `t1`` |
| 76 | reparse-failed:ParsingException: Getting syntax error at line 2, column 34. Detail message: No viable statement for input 'WITH `t0` AS (SELECT `c1` FROM (VALUES(PseudoType.AnyMapType{'. | `WITH `t0` AS (SELECT `c1` FROM (VALUES(PseudoType.AnyMapType{})) t(c1) GROUP BY `c1`) SELECT map_concat(PseudoType.AnyMapType{'a':1,'b':2}, `c1`) FROM `t0`` |
| 71 | reparse-failed:ParsingException: Getting syntax error at line 1, column 36. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT adddate(PseudoType.AnyMapType{}, INTERVAL 1 * 2 YEAR)` |
| 71 | reparse-failed:ParsingException: Getting syntax error at line 1, column 35. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT typeof(PseudoType.AnyMapType{1:'apple',2:'map<tinyint,varchar>',3:'pear'})` |
| 70 | reparse-failed:ParsingException: Getting syntax error at line 3, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM `test_array_contains` WHERE array_contains(`array_int`, PseudoType.AnyMapType{}) ORDER BY `id` ASC` |
| 69 | reparse-failed:ParsingException: Getting syntax error at line 5, column 37. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT * FROM (SELECT `user_id` + 1, `time`, sum(`tag_id`) FROM `user_tags` GROUP BY `user_id` + 1, `time`) `t` WHERE `time` = (PseudoType.AnyMapType{})` |
| 65 | reparse-failed:ParsingException: Getting syntax error at line 1, column 34. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT min_n(PseudoType.AnyMapType{}, 3)` |
| 56 | reparse-failed:ParsingException: Getting syntax error at line 3, column 35. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT max(`id`), array_agg(DISTINCT `name`), count(DISTINCT `id`), array_agg(`name`) FROM `ss` WHERE `id` < (PseudoType.AnyMapType{}) ORDER BY 1 ASC` |
| 52 | reparse-failed:ParsingException: Getting syntax error at line 1, column 188. Detail message: No viable statement for input '((CAST((CAST((PseudoType.AnyMapType{'. | `SELECT CASE WHEN ((CAST((CAST((ARRAY<INT>[1, 2, 3]) AS VARIANT)) AS VARCHAR)) = '[1,2,3]') THEN 'PASS' ELSE 'FAIL' END AS `v_array_roundtrip`, CASE WHEN ((CAST((CAST((PseudoType.AnyMapType{'b':2,'a':1}) AS VARIANT)) AS VARCHAR)) = '{"a":1,"b":2}') THEN 'PASS' ELSE 'FAIL' END AS `v_map_roundtrip`, CASE WHEN ((CAST((CAST((row(1, 'x')) AS VARIANT)) AS VARCHAR)) = '{"col1":1,"col2":"x"}') THEN 'PASS'  ...` |
| 48 | reparse-failed:ParsingException: Getting syntax error at line 2, column 52. Detail message: Unexpected input 'ON', the most similar input is {<EOF>, ';'}. | `SELECT `t`.`map2`, `s`.`map2` FROM `map_test` AS `t` INNER JOIN `map_test` AS `s` ON (map_apply((k, v) -> PseudoType.AnyMapType{`k` + 1:array_length(`v`)}, `s`.`map2`)) = (map_apply((k, v) -> PseudoType.AnyMapType{`k` + 1:array_length(`v`)}, `k`)) ORDER BY `t`.`pk` ASC` |
| 47 | reparse-failed:ParsingException: Getting syntax error at line 4, column 37. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT * FROM (SELECT max(`v1`) OVER (PARTITION BY `v2` ) AS `mv1` FROM `t1`) `t` WHERE `mv1` != (PseudoType.AnyMapType{}) ORDER BY `mv1` DESC  LIMIT 1` |
| 46 | reparse-failed:ParsingException: Getting syntax error at line 1, column 39. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT hours_diff(PseudoType.AnyMapType{}, '2020-01-01 00:00:00')` |

| Outcome | Count |
|---|---:|
| OK | 344789 |
| ANALYZE_REJECTED | 232980 |
| ANALYZE_INTERNAL_ERROR | 321 |
| DEPARSE_THROW | 0 |
| REPARSE_FAIL | 0 |
| REANALYZE_FAIL | 75 |
| FIXPOINT_MISMATCH | 162 |

## Bug candidates (5 distinct signatures)

### ANALYZE_INTERNAL_ERROR — UnsupportedException@com.starrocks.sql.common.UnsupportedException#unsupportedException (x320)

- detail: UnsupportedException: Table function cannot appear on the left side of a join. Place it on the right side (optionally with LATERAL) or wrap it with TABLE(...).
- seed:     with w_empty as (select * from t1 where c1 = 'not-exist'), w1 as (select k1, k2, count(1) as cnt from t1 group by k1, k2), w2 as (select k1, k2, c1, count(1) as cnt from t1 group by k1, k2, c1) select count(1) from t1 tt1 join w_empty tt2 join t1 tt3 join w1 tt4 join w2 tt5
- mutation: M6-nesting TABLE_FUNCTION at JoinRelation.left: `t1` AS `tt1` INNER JOIN `w_empty` AS `tt2` INNER JOIN `t1` AS `tt3` INNER JOIN `w1` AS `tt4` -> (SELECT * FROM `t1` AS `tt1` INNER JOIN `w_empty` AS `tt2` INNER JOIN `t1` AS `tt3` INNER JOIN `w1` AS `tt4`) `srfuzz_n1`, generate_series(1, 3) `srfuzz_n2 ...
- mutant:   WITH `w_empty` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, `srfuzz_mut_7`.`t1`.`c1` FROM `srfuzz_mut_7`.`t1` WHERE `srfuzz_mut_7`.`t1`.`c1` = 'not-exist') , `w1` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, count(1) AS `cnt` FROM `srfuzz_mut_7`.`t1` GROUP BY `srf ...

### REANALYZE_FAIL — SemanticException@com.starrocks.sql.analyzer.Scope#resolveField (x72)

- detail: SemanticException: Getting analyzing error. Detail message: Column 'id2' is ambiguous.
- seed:     SELECT * FROM left_table JOIN right_table USING(id1) ORDER BY id1
- mutation: M6-nesting SUBQUERY at SelectRelation.from: `left_table` INNER JOIN `right_table` USING (`id1`) -> (SELECT * FROM `left_table` INNER JOIN `right_table` USING (`id1`)) `srfuzz_n1`
- mutant:   SELECT `srfuzz_n1`.`id1`, `srfuzz_n1`.`name`, `srfuzz_n1`.`age`, `srfuzz_n1`.`city`, `srfuzz_n1`.`id2`, `srfuzz_n1`.`salary`, `srfuzz_n1`.`status`, `srfuzz_n1`.`dept`, `srfuzz_n1`.`bonus`, `srfuzz_n1`.`id2` FROM (SELECT `srfuzz_mut_664`.`left_table`.`id1`, `srfuzz_mut_664`.`left_table`.`name`, `srfu ...

### REANALYZE_FAIL — SemanticException@com.starrocks.sql.analyzer.AggregationAnalyzer#analyze (x2)

- detail: SemanticException: Getting analyzing error from line 1, column 75 to line 1, column 267. Detail message: 'array_map(x -> array_concat([x], CAST(if(1 > rand(), '[]', '') AS ARRAY<VARCHAR(65533)>)), `srfuzz_mut_120`.`arra...
- seed:     select cast(if (1 > rand(), "[]", "") as array<string>) l , array_map((x)-> (concat(x,l)), arr_str) from array_map_x
- mutation: M5-clause select#0: add GROUP BY CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>), array_map(x -> concat(`x`, `l`), `arr_str`)
- mutant:   SELECT CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>) AS `l`, array_map(x -> array_concat(ARRAY<VARCHAR(65533)>[`__LAMBDA_TABLE`.`__LAMBDA_TABLE`.`x`], CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>)), `srfuzz_mut_120`.`array_map_x`.`arr_str`) AS `array_map(x -> concat(x, CA ...

### REANALYZE_FAIL — SemanticException@com.starrocks.sql.analyzer.FunctionAnalyzer#getAdjustedAnalyzedFunction (x1)

- detail: SemanticException: Getting analyzing error. Detail message: No matching function with signature: bucket(float, varchar).
- seed:     select __iceberg_transform_bucket(10.65, 8) from t0
- mutation: FunctionCallExpr[1]: 8 -> '1970-01-01' | M9-session: sql_mode=131104
- mutant:   SELECT __iceberg_transform_bucket(10.65, '1970-01-01') AS `__iceberg_transform_bucket(10.65, '1970-01-01')` FROM `srfuzz_mut_431`.`t0`

### ANALYZE_INTERNAL_ERROR — StarRocksPlannerException@com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions#str2Date (x1)

- detail: StarRocksPlannerException: Fail to parse date
- seed:     select str_to_date('invalid', '%Y-%m-%d')
- mutation: FunctionCallExpr[1]: '%Y-%m-%d' -> str_to_date('invalid', '%Y-%m-%d') | M9-session: sql_mode=544
- mutant:   SELECT str_to_date('invalid', str_to_date('invalid', '%Y-%m-%d'))

## Analyzer rejections (8576 signatures, 232980 instances)

### By operator

| operator | instances | share | signatures | most common reason |
|---|---:|---:|---:|---|
| M1-M4-expr | 98245 | 42.2% | 4058 | Unknown table 'S'. |
| M7-typestress | 75121 | 32.2% | 3687 | Unknown table 'S'. |
| M5-clause | 33513 | 14.4% | 774 | Column 'S' cannot be resolved. |
| M6-nesting | 26101 | 11.2% | 57 | Column 'S' cannot be resolved. |

### Highest volume (40 of 8576 signatures)

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 23865 | M1-M4-expr | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | BinaryPredicate[0]: `k1` -> `event_day` |
| 22546 | M1-M4-expr | SemanticException@AggregationAnalyzer$VerifyExpressionVisitor#visitFunctionCall | Unsupported nest aggregation function inside aggregation. | FunctionCallExpr[0]: 1 -> count(1) |
| 17541 | M1-M4-expr | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | BinaryPredicate[0]: `k2` -> `column_name` |
| 10579 | M5-clause | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M5-clause select#0: add ORDER BY `usage` DESC NULLS LAST |
| 9787 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M6-nesting ASOF_JOIN at SelectRelation.from: `t3_predicate` -> (SELECT * FROM `t3_predicate`) `t3_predicate` ASOF LEFT JOIN (SELECT * FROM `t3_predicate`) `srfuzz_n1` ON `t3_predicate`.`k2` = `srfuzz_n1`.`k2` AND `t3_predic ... |
| 8554 | M5-clause | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M5-clause select#0: add GROUP BY `k1` |
| 7987 | M6-nesting | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M6-nesting ASOF_JOIN at SelectRelation.from: `target_table` -> (SELECT * FROM `target_table`) `target_table` ASOF LEFT JOIN (SELECT * FROM `target_table`) `srfuzz_n1` ON `target_table`.`event_day` = `srfuzz_n1`.`event_day` ... |
| 7215 | M7-typestress | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M7-typestress JSON_PATH at BinaryPredicate[1]: '2021-01-01' -> json_length(CAST(('2021-01-01') AS JSON)) \|\| marker: json_length(CAST('2021-01-01' AS JSON)) \| M9-session: enable_groupby_use_output_alias=true |
| 5957 | M7-typestress | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M7-typestress STRUCT_FIELD at BinaryPredicate[0]: `c1` -> (`c1`).a \|\| marker: `c1`.`a` |
| 5801 | M1-M4-expr | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | BinaryPredicate[1]: `tt5`.`k1` -> `k1` |
| 5570 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at WhereClause: `c1` = 'not-exist' -> (`c1` = 'not-exist')[2147483647] \|\| marker: `c1` = 'not-exist'[2147483647] |
| 5514 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | M6-nesting SELF_JOIN at SelectRelation.from: `information_schema`.`column_stats_usage` -> (SELECT * FROM `information_schema`.`column_stats_usage`) `column_stats_usage` RIGHT OUTER JOIN (SELECT * FROM `information_schema`.`column_stats_usage`) `srfuz ... \| M9-session: enable_groupby_use_output_alias ... |
| 4174 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BIGINT because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: count(1) -> (count(1))[CAST(NULL AS VARCHAR)] \|\| marker: count(1)[CAST(NULL AS VARCHAR)] |
| 4134 | M5-clause | SemanticException@SelectAnalyzer#analyze | cannot combine SELECT DISTINCT with aggregate functions without GROUP BY. | M5-clause select#3: add DISTINCT |
| 3353 | M5-clause | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | M5-clause select#2: remove GROUP BY `k1`, `k2`, `c1` |
| 3167 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type INT because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at BinaryPredicate[0]: `k2` -> (`k2`)[-1] \|\| marker: `k2`[-1] |
| 3123 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type TINYINT because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: 1 -> (1)[''] \|\| marker: 1[''] |
| 2097 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | 'S' must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at BinaryPredicate[1]: 'test_overwrite_statistics_behavior' -> ('test_overwrite_statistics_behavior').c \|\| marker: 'test_overwrite_statistics_behavior'.`c` \| M9-session: enable_groupby_use_output_alias=true |
| 1753 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedFunction | Table function cannot be used in expression. | M7-typestress JSON_PATH at BinaryPredicate[0]: `tt1`.`k1` -> json_each(CAST((`tt1`.`k1`) AS JSON)) \|\| marker: json_each(CAST(`tt1`.`k1` AS JSON)) |
| 1720 | M1-M4-expr | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | BinaryPredicate[0]: `c1` -> count(*) |
| 1515 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR(N) because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at BinaryPredicate[0]: `table_database` -> (`table_database`)[-2147483648] \|\| marker: `table_database`[-2147483648] |
| 1475 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DECIMAL128(N,N) because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[3]: `c4` -> (`c4`)[2] \|\| marker: `c4`[2] |
| 1443 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DECIMAL64(N,N) because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at FunctionCallExpr[0]: `c1` -> (`c1`)[1] \|\| marker: `c1`[1] |
| 1180 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_slice should be an array. | M7-typestress COLLECTION_FN at FunctionCallExpr[0]: 1 -> array_slice((1), 1, 3) \|\| marker: array_slice(1, 1, 3) |
| 933 | M5-clause | SemanticException@AnalyzerUtils#verifyNoWindowFunctions | WHERE clause cannot contain window function. | M5-clause select#0: add WHERE row_number() OVER (PARTITION BY `srfuzz_mut_263`.`decimal_window_test`.`category` ORDER BY `srfuzz_mut_263`.`decimal_window_test`.`d76_20` ASC ) |
| 878 | M1-M4-expr | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | FunctionCallExpr[0]: sum(murmur_hash3_32(`__col_1`)) -> sign(murmur_hash3_32(`__col_1`)) |
| 840 | M1-M4-expr | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | JOIN clause cannot contain aggregations. | BinaryPredicate[0]: `tt1`.`k1` -> count(1) |
| 838 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BOOLEAN because it is not an array or a map or a struct. | M7-typestress MAP_KEY at WhereClause: (`c_id` >= 71) AND (`c_id` <= 90) -> ((`c_id` >= 71) AND (`c_id` <= 90))[1] \|\| marker: (`c_id` >= 71) AND (`c_id` <= 90)[1] |
| 832 | M5-clause | SemanticException@AnalyzerUtils#verifyNoWindowFunctions | GROUP BY clause cannot contain window function. | M5-clause select#1: add GROUP BY `wv` |
| 736 | M6-nesting | SemanticException@QueryAnalyzer$AsofJoinConditionValidator#validateTemporalConditionTypes | ASOF JOIN temporal condition supports only BIGINT, DATE, or DATETIME types in join ON clause, found: int(N) and int(N). predicate: `ID`.`ID` > `ID`.`ID`. | M6-nesting ASOF_JOIN at JoinRelation.right: `t1` AS `tt3` -> (SELECT * FROM `t1` AS `tt3`) `tt3` ASOF LEFT JOIN (SELECT * FROM `t1` AS `tt3`) `srfuzz_n1` ON `tt3`.`c1` = `srfuzz_n1`.`c1` AND `tt3`.`k1` > `srfuzz_n1`.`k1` |
| 720 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | array subscript must have type integer. | M7-typestress MAP_KEY at FunctionCallExpr[0]: `a1` -> (`a1`)[''] \|\| marker: `a1`[''] |
| 713 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type JSON because it is not an array or a map or a struct. | M7-typestress MAP_KEY at FunctionCallExpr[0]: json_object('2:3') -> (json_object('2:3'))[CAST(NULL AS VARCHAR)] \|\| marker: json_object('2:3')[CAST(NULL AS VARCHAR)] |
| 701 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | M5-clause select#0: add WHERE count(*) |
| 612 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATE because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[5]: `c6` -> (`c6`)[-1] \|\| marker: `c6`[-1] |
| 551 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DOUBLE because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[3]: `c12` -> (`c12`)[CAST(NULL AS INT)] \|\| marker: `c12`[CAST(NULL AS INT)] |
| 535 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATETIME because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: `c7` -> (`c7`)['a.b'] \|\| marker: `c7`['a.b'] |
| 505 | M5-clause | SemanticException@SelectAnalyzer#lambda$analyzeSelect$8 | DISTINCT can only be applied to comparable types : JSON. | M5-clause select#0: add DISTINCT |
| 498 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | GROUP BY clause cannot contain aggregations. | M5-clause select#0: add GROUP BY percentile_disc_lc(`v1`, 0.5) |
| 447 | M5-clause | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | M5-clause select#3: add GROUP BY `c1` \| M9-session: sql_mode=288 |
| 427 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_position should be an array. | FunctionCallExpr[0]: <ArrayExpr> -> `srfuzz_mut_115`.`t`.`pk` \| M9-session: sql_mode=34 |

8536 further signatures (62164 instances) below this cut.

### Rare signatures, count <= 2 (5665 of them, 6978 instances)

A false rejection would be here rather than above: one the analyzer should not have
issued is by nature uncommon. Read the message against the mutation next to it and ask
whether the mutant really was invalid.

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 1 | M5-clause | SemanticException@AnalyticAnalyzer#verifyAnalyticExpression | Expressions in the PARTITION BY clause must not be constant: N (in min(N) OVER (PARTITION BY N)). | M5-clause select#0: add window over item 0 (1 -> min(1) OVER (PARTITION BY 2)) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from map<varchar(N),map<int(N),array<json>>> to bigint(N) in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: 1 -> CAST(CAST(CAST((1) AS JSON) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) AS BIGINT) \|\| marker: CAST((CAST((CAST(1 AS JSON)) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>)) AS BIGINT) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeWhere | WHERE clause CAST(`ID`.`ID`.`ID` = 'S' AS ARRAY<INT>) can not be converted to boolean type. | M7-typestress CAST_CHAIN at WhereClause: `c1` = 'not-exist' -> CAST((`c1` = 'not-exist') AS ARRAY<INT>) \|\| marker: CAST((`c1` = 'not-exist') AS ARRAY<INT>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | md5(group_concat(((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)), 'S')) must be | M7-typestress STRUCT_FIELD at SelectListItem[1]: md5(group_concat(((CAST(`c2` AS VARCHAR(65533))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',')) -> (md5(group_concat(((CAST(`c2` AS VARCHAR(65533))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ','))).`no such field` \|\| marker: md5 ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCompoundPredicate | CAST(((((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN))) OR (CAST('S' AS BOOLEAN | M7-typestress CAST_CHAIN at CompoundPredicate[0]: ((((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHAR(65533)))) OR ':') OR `c8` -> CAST((((((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHAR(65533)))) OR ':') OR `c8`) AS MAP<INT, ARRAY<INT>>) \|\| marker: CAST((((((CAST(`c6 ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCompoundPredicate | CAST(CAST(CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN) AS ARRAY<VARCHAR(N)>) AS ARRAY<VARCHAR(N)>) can not be converted to boolean type.. | M7-typestress CAST_CHAIN at CompoundPredicate[0]: CAST(`c1` AS VARCHAR(65533)) -> CAST(CAST(CAST((CAST(`c1` AS VARCHAR(65533))) AS BOOLEAN) AS ARRAY<VARCHAR(1)>) AS ARRAY<VARCHAR(1)>) \|\| marker: CAST((CAST((CAST((CAST(`c1` AS VARCHAR(65533))) AS BOOLEAN)) AS ARRAY<VARCHAR(1)>)) AS ARRAY<VARCHAR(1)>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | group_concat(((CAST(`ID`.`ID` AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)), 'S') must be a struct type, check if  | M7-typestress STRUCT_FIELD at FunctionCallExpr[0]: group_concat((`c8` OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',') -> (group_concat((`c8` OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',')).col \|\| marker: group_concat((`c8` OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',' ... |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(CAST(`ID`.`ID`.`ID` AS INT) AS JSON)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c3` -> CAST(CAST((`c3`) AS INT) AS JSON) \|\| marker: CAST((CAST(`c3` AS INT)) AS JSON) \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(CAST(`ID`.`ID`.`ID` AS INT) AS DATE)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c1` -> CAST(CAST((`c1`) AS INT) AS DATE) \|\| marker: CAST((CAST(`c1` AS INT)) AS DATE) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: avg(array<bigint(N)>). | FunctionCallExpr[0]: `c3` -> array_agg(DISTINCT `srfuzz_mut_19`.`skew_agg`.`c2`) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | count(DISTINCT CAST(`ID`.`ID`.`ID` AS JSON)->'S', `ID`.`ID`.`ID`) can't rewrite distinct to group by on (json,varchar(N)). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c0` -> CAST((`c0`) AS JSON) -> '$' \|\| marker: CAST(`c0` AS JSON)->'$' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | count(DISTINCT `ID`.`ID`.`ID`, `ID`.`ID`.`ID`, `ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: count(DISTINCT `c1`, `c2`, `c3`) -> (count(DISTINCT `c1`, `c2`, `c3`)).b \|\| marker: count(DISTINCT `c1`, `c2`, `c3`).`b` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | bitmap_union_count(to_bitmap(`ID`.`ID`.`ID` + `ID`.`ID`.`ID`)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[1]: bitmap_union_count(to_bitmap(`c0` + `c3`)) -> (bitmap_union_count(to_bitmap(`c0` + `c3`))).`c4` \|\| marker: bitmap_union_count(to_bitmap(`c0` + `c3`)).`c4` |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | count(DISTINCT `ID`.`ID`.`ID`, CAST(`ID`.`ID`.`ID` AS JSON)->'S') can't rewrite distinct to group by on (int(N),json). | M7-typestress JSON_PATH at FunctionCallExpr[1]: `c5` -> CAST((`c5`) AS JSON) -> '$.a' \|\| marker: CAST(`c5` AS JSON)->'$.a' |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(DISTINCT date_trunc('S', `ID`.`ID`.`ID`)). | FunctionCallExpr[0]: length(`c2`) -> date_trunc('week', `srfuzz_mut_19`.`skew_agg`.`c4`) \| M9-session: sql_mode=544 |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(CAST(`ID`.`ID`.`ID` AS VARCHAR), sum(DISTINCT length(`ID`.`ID`.`ID`))). | FunctionCallExpr[1]: ',' -> sum(DISTINCT length(`srfuzz_mut_19`.`skew_agg`.`c2`)) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | BITMAP_UNION_INT params only support Integer getType(). | FunctionCallExpr[0]: CAST((length(`c1`)) AS INT) -> avg(DISTINCT `srfuzz_mut_19`.`skew_agg`.`c1`) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | CAST(`ID`.`ID`.`ID` AS BIGINT) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at FunctionCallExpr[0]: CAST(`c4` AS BIGINT) -> (CAST(`c4` AS BIGINT)).a \|\| marker: CAST(`c4` AS BIGINT).`a` |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(CAST(`ID`.`ID`.`ID` AS VARCHAR), hll_union_agg(hll_hash(`ID`.`ID`.`ID`))). | FunctionCallExpr[1]: ',' -> hll_union_agg(hll_hash(`srfuzz_mut_19`.`skew_agg`.`c5`)) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(CAST(`ID`.`ID`.`ID` AS VARCHAR), N). | FunctionCallExpr[1]: ',' -> 0.0 |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | ifnull(sum(murmur_hash3_32(`ID`.`ID`)), N) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at ArithmeticExpr[1]: ifnull(sum(murmur_hash3_32(`c0`)), 0) -> (ifnull(sum(murmur_hash3_32(`c0`)), 0)).c \|\| marker: ifnull(sum(murmur_hash3_32(`c0`)), 0).`c` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(DISTINCT length(`ID`.`ID`.`ID`)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[3]: sum(DISTINCT length(`c0`)) -> (sum(DISTINCT length(`c0`))).`__col_0` \|\| marker: sum(DISTINCT length(`c0`)).`__col_0` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<array<varchar>> to date in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: array_agg(`a1`) -> CAST((array_agg(`a1`)) AS DATE) \|\| marker: CAST((array_agg(`a1`)) AS DATE) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: map_size(array<varchar>). | M7-typestress COLLECTION_FN at SelectListItem[0]: split('a,b,c', ',') -> map_size((split('a,b,c', ','))) \|\| marker: map_size(split('a,b,c', ',')) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | count(DISTINCT `ID`.`ID`.`ID`, `ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: count(DISTINCT `c1`, `c2`) -> (count(DISTINCT `c1`, `c2`)).a \|\| marker: count(DISTINCT `c1`, `c2`).`a` |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT json_query(CAST(`ID`.`ID`.`ID` AS JSON), 'S'), 'S') can't rewrite distinct to group by on (json,varchar). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c2` -> json_query(CAST((`c2`) AS JSON), '$.a') \|\| marker: json_query(CAST(`c2` AS JSON), '$.a') |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bitmap, varchar). | FunctionCallExpr[0]: `c2` -> to_bitmap(`srfuzz_mut_19`.`skew_agg`.`c0` + `srfuzz_mut_19`.`skew_agg`.`c3`) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bigint(N), array<varchar(N)>, varchar, largeint(N)). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: upper(`c4`) -> CAST((upper(`c4`)) AS ARRAY<VARCHAR(1)>) \|\| marker: CAST((upper(`c4`)) AS ARRAY<VARCHAR(1)>) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, upper(`ID`.`ID`.`ID`), TRUE ORDER BY abs(`ID`.`ID`.`ID` + `ID`. | FunctionCallExpr[2]: ',' -> TRUE |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT N, CAST('S' AS BIGINT)). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: ',' -> CAST((',') AS BIGINT) \|\| marker: CAST(',' AS BIGINT) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, CAST(CAST(CAST('S' AS DATETIME) AS SMALLINT) AS DATETIME)). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: ',' -> CAST(CAST(CAST((',') AS DATETIME) AS SMALLINT) AS DATETIME) \|\| marker: CAST((CAST((CAST(',' AS DATETIME)) AS SMALLINT)) AS DATETIME) \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT json_query(CAST(`ID`.`ID` AS JSON), 'S'), upper(`ID`.`ID`), 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) can't rewrite distinct to group b | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c2` -> json_query(CAST((`c2`) AS JSON), '$.a') \|\| marker: json_query(CAST(`c2` AS JSON), '$.a') |
| 1 | M1-M4-expr | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT CAST(NULL AS JSON), 'S') can't rewrite distinct to group by on (json,varchar). | FunctionCallExpr[0]: 1 -> CAST(NULL AS JSON) \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT N, FALSE). | FunctionCallExpr[1]: ',' -> FALSE |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bigint(N), array<varchar>). | FunctionCallExpr[1]: ',' -> split('a,b,c', ',') |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, upper(`ID`.`ID`.`ID`), CAST('S' AS JSON)->'S' ORDER BY abs(`ID` | M7-typestress JSON_PATH at FunctionCallExpr[2]: ',' -> CAST((',') AS JSON) -> '$' \|\| marker: CAST(',' AS JSON)->'$' |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT CAST(N AS JSON)->'S', 'S') can't rewrite distinct to group by on (json,varchar). | M7-typestress JSON_PATH at FunctionCallExpr[0]: 1 -> CAST((1) AS JSON) -> '$' \|\| marker: CAST(1 AS JSON)->'$' |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT `ID`.`ID`, json_query(CAST(upper(`ID`.`ID`) AS JSON), 'S'), 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) can't rewrite distinct to group b | M7-typestress JSON_PATH at FunctionCallExpr[1]: upper(`c4`) -> json_query(CAST((upper(`c4`)) AS JSON), '$.a') \|\| marker: json_query(CAST((upper(`c4`)) AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), CAST(CAST('S' AS BOOLEAN) AS DECIMAL128(N,N)) ORDE | M7-typestress CAST_CHAIN at FunctionCallExpr[2]: ',' -> CAST(CAST((',') AS BOOLEAN) AS DECIMAL(38, 37)) \|\| marker: CAST((CAST(',' AS BOOLEAN)) AS DECIMAL128(38,37)) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT `ID`.`ID`, CAST(upper(`ID`.`ID`) AS JSON)->'S', 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) can't rewrite distinct to group by on (bigint | M7-typestress JSON_PATH at FunctionCallExpr[1]: upper(`c4`) -> CAST((upper(`c4`)) AS JSON) -> '$.a' \|\| marker: CAST((upper(`c4`)) AS JSON)->'$.a' \| M9-session: sql_mode=288 |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, json_query(CAST('S' AS JSON), 'S')). | M7-typestress JSON_PATH at FunctionCallExpr[1]: ',' -> json_query(CAST((',') AS JSON), '$.a') \|\| marker: json_query(CAST(',' AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, upper(`ID`.`ID`.`ID`), json_query(CAST('S' AS JSON), 'S') ORDER | M7-typestress JSON_PATH at FunctionCallExpr[2]: ',' -> json_query(CAST((',') AS JSON), '$.a') \|\| marker: json_query(CAST(',' AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT `ID`.`ID`.`ID`, CAST(upper(`ID`.`ID`.`ID`) AS JSON)->'S', 'S' ORDER BY abs(`ID`.`ID`.`ID` + `ID`.`ID`.`ID`) ASC) can't rewrite distinct to | M7-typestress JSON_PATH at FunctionCallExpr[1]: upper(`c4`) -> CAST((upper(`c4`)) AS JSON) -> '$.a' \|\| marker: CAST((upper(`c4`)) AS JSON)->'$.a' |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, N + N). | FunctionCallExpr[1]: ',' -> 1 + 1 |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, json_length(CAST('S' AS JSON))). | M7-typestress JSON_PATH at FunctionCallExpr[1]: ',' -> json_length(CAST((',') AS JSON)) \|\| marker: json_length(CAST(',' AS JSON)) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), CAST('S' AS DECIMAL128(N,N)) ORDER BY abs(`ID`.`ID | M7-typestress CAST_CHAIN at FunctionCallExpr[2]: ',' -> CAST((',') AS DECIMAL(38, 0)) \|\| marker: CAST(',' AS DECIMAL128(38,0)) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from struct<`ID` int(N)> to boolean in sql `ID`a`ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c3` -> CAST(CAST(CAST((`c3`) AS JSON) AS STRUCT<a INT>) AS BOOLEAN) \|\| marker: CAST((CAST((CAST(`c3` AS JSON)) AS struct<`a` int(11)>)) AS BOOLEAN) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitBinaryPredicate | Column type array<tinyint(N)> does not support binary predicate operation with type varchar(N). | BinaryPredicate[0]: `t0`.`c4` -> array_agg(DISTINCT 2) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT 'S', 'S', N). | FunctionCallExpr[2]: ',' -> 0 |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeWhere | WHERE clause CAST(CAST(N = N AS ARRAY<MAP<VARCHAR(N),INT>>) AS ARRAY<MAP<VARCHAR(N),INT>>) can not be converted to boolean type. | M7-typestress CAST_CHAIN at WhereClause: 1 = 0 -> CAST(CAST((1 = 0) AS ARRAY<MAP<VARCHAR(16), INT>>) AS ARRAY<MAP<VARCHAR(16), INT>>) \|\| marker: CAST((CAST((1 = 0) AS ARRAY<MAP<VARCHAR(16),INT>>)) AS ARRAY<MAP<VARCHAR(16),INT>>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitBinaryPredicate | Column type array<map<varchar(N),int(N)>> does not support binary predicate operation with type tinyint(N). | M7-typestress CAST_CHAIN at BinaryPredicate[0]: 1 -> CAST(CAST((1) AS VARCHAR) AS ARRAY<MAP<VARCHAR(16), INT>>) \|\| marker: CAST((CAST(1 AS VARCHAR)) AS ARRAY<MAP<VARCHAR(16),INT>>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(if(`ID`.`ID` < N, `ID`.`ID`, `ID`.`ID` + N)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at ArithmeticExpr[1]: sum(if(`xx` < 1, `v2`, `v1` + 1)) -> (sum(if(`xx` < 1, `v2`, `v1` + 1))).`v3` \|\| marker: sum(if(`xx` < 1, `v2`, `v1` + 1)).`v3` |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: json_query(boolean, bigint(N), bigint(N)). | FunctionCallExpr[0]: if(`xx` < 1, `v2`, `v1` + 1) -> json_query(`xx` < 1, `v2`, `v1` + 1) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer#getArithmeticFunction | cast type array<json> with type largeint(N) is invalid. | M7-typestress CAST_CHAIN at ArithmeticExpr[0]: `v` -> CAST(CAST((`v`) AS CHAR(1)) AS ARRAY<JSON>) \|\| marker: CAST((CAST(`v` AS CHAR(1))) AS ARRAY<JSON>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(`ID`.`ID`.`ID` + N - `ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: sum((`v` + 18446744073709551616) - `v`) -> (sum((`v` + 18446744073709551616) - `v`)).`a` \|\| marker: sum((`v` + 18446744073709551616) - `v`).`a` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | max(length(`ID`.`ID`)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: max(length(`c0`)) -> (max(length(`c0`))).`c1` \|\| marker: max(length(`c0`)).`c1` |
| 1 | M7-typestress | SemanticException@DecimalV3FunctionAnalyzer#getDecimalV3Function | No matching function with signature: round(struct<`ID` int(N), `ID` varchar(N)>, tinyint(N)).. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: var_samp(`val1`) -> CAST(CAST(CAST((var_samp(`val1`)) AS VARCHAR(1)) AS JSON) AS STRUCT<a INT, b VARCHAR(8)>) \|\| marker: CAST((CAST((CAST((var_samp(`val1`)) AS VARCHAR(1))) AS JSON)) AS struct<`a` int(11), `b` varchar(8)>) \| M9-session: enable_groupby ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: variance_samp(array<boolean>). | M7-typestress COLLECTION_FN at FunctionCallExpr[0]: `c1` -> array_sort((`c1`)) \|\| marker: array_sort(`c1`) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | round(stddev_samp(`ID`.`ID`.`ID`) OVER (ORDER BY `ID`.`ID`.`ID` ASC ROWS BETWEEN N PRECEDING AND CURRENT ROW), N) must be a struct type, check if you are using  | M7-typestress STRUCT_FIELD at SelectListItem[2]: round(stddev_samp(`val1`) OVER (ORDER BY `k` ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 3) -> (round(stddev_samp(`val1`) OVER (ORDER BY `k` ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 3)).`a` \|\| marker: round(stddev_samp(`val1`) OVER (ORDER BY  ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | round(covar_samp(`ID`.`ID`, `ID`.`ID`), N) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: round(covar_samp(`c1`, `c2`), 3) -> (round(covar_samp(`c1`, `c2`), 3)).`k` \|\| marker: round(covar_samp(`c1`, `c2`), 3).`k` |

5605 further rare signatures not shown.


## Non-defect signature histogram (82 signatures, 162 instances)

| count | outcome | signature |
|---:|---|---|
| 52 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` |
| 9 | FIXPOINT_MISMATCH | fixpoint:BY `ID`.`ID`, `ID` => BY `ID`.`ID`, `ID`.`ID` |
| 5 | FIXPOINT_MISMATCH | fixpoint:((((`ID`.`ID`.`ID` * N) + N.) => ((((`ID`.`ID`.`ID` * N) + N) + N) + N) |
| 3 | FIXPOINT_MISMATCH | fixpoint:SELECT N * N AS `N => SELECT N * N AS `N |
| 3 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` LIMIT N, N => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` LIMIT N, N |
| 3 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`, `ID` => `ID`.`ID`, `ID`.`ID` |
| 3 | FIXPOINT_MISMATCH | fixpoint:lag(`ID`.`ID`.`ID`, N, -N.) OVER (ORDER BY `ID`.`ID`.`ID` ASC ) AS `ID => lag(`ID`.`ID`.`ID`, N, -N) OVER (ORDER BY `ID`.`ID`.`ID` ASC ) AS `ID` |
| 2 | FIXPOINT_MISMATCH | fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) |
| 2 | FIXPOINT_MISMATCH | fixpoint:((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID => ((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID |
| 2 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` >= N => `ID`.`ID`.`ID` >= N ORDER BY `ID`.`ID`.`ID` ASC |
| 2 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` = N) => (`ID`.`ID`.`ID` = N) ORDER BY `ID`.`ID`.`ID` |
| 2 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` >= N)) AND ((`ID`.`ID`.`huge_decimal => (`ID`.`ID`.`ID` >= N)) AND ((`ID`.`ID`.`ID` IS NOT |
| 2 | FIXPOINT_MISMATCH | fixpoint:N AS `ID` FROM `ID`.`ID` => N AS `ID` FROM `ID`.`ID` |
| 2 | FIXPOINT_MISMATCH | fixpoint:CAST(N AS DECIMAL(N,N)) AS `ID`, CAST(N AS DECIMAL64(N,N)) => CAST(N AS DECIMAL64(N,N)) AS `ID`, CAST(N AS DECIMAL64(N,N)) |
| 2 | FIXPOINT_MISMATCH | fixpoint:get_json_string(CAST(('S') AS JSON), 'S') AS `get_json_string(CAST((ty => get_json_string(CAST('S' AS JSON), 'S') AS `get_json_string(CAST((type |
| 2 | FIXPOINT_MISMATCH | fixpoint:lag(`ID`.`ID`.`ID`, N, -N.) OVER (PARTITION BY `ID`.`ID`.`ID` ORDER BY => lag(`ID`.`ID`.`ID`, N, -N) OVER (PARTITION BY `ID`.`ID`.`ID` ORDER BY  |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`)[N.] AS `ID` FROM `ID`.`ID` => `ID`.`ID`.`ID`)[N.] AS `ID` FROM `ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:- N)) / N) < N AS `ID`, `ID`.`ID` => - N)) / N) < N AS `ID`, `ID`.`ID` = |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, array_agg(N) OVER (PARTITION BY `ID`.`ID`.`ID` => `ID`.`ID`.`ID`, array_agg(N) OVER (PARTITION BY `ID`.`ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a => fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a |
| 1 | FIXPOINT_MISMATCH | fixpoint:ray_position(`ID`.`ID`, N) AS `ID` FROM `ID` AS test_array_contains => ray_position(`ID`.`ID`, N) AS `ID` FROM `ID` AS test_array_contains |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT crc32_hash([MAP<TINYINT,BOOLEAN>{N:NULL}]) AS `ID` => SELECT crc32_hash(ARRAY<MAP<TINYINT,BOOLEAN>>[MAP<TINYINT,BOOLEAN>{N:N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * N AS => SELECT N * N AS `N |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S' * N AS `'S' => 'S' * N AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * (get_json_string(CAST(N => SELECT N * (get_json_string(CAST(N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT (N * N) => SELECT (N * N) * N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT 'S' * N AS `ID` => SELECT 'S' * N AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT (N * N) * N AS `(N * N) => SELECT (N * N) * N AS `(N * N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:CAST((((-N * (-N + N.)) => CAST((((-N * (-N + N)) - N) - N) AS VARCHAR(N)) AS `CAST((((-N |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` < N => `ID`.`ID`.`ID` < N ORDER BY `ID`.`ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` < N. => `ID`.`ID`.`ID` < N ORDER BY `ID`.`ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:((CAST(NULL AS JSON)), N) => ((CAST(NULL AS JSON)), N) ORDER BY `ID`.`ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` NOT IN (N., => `ID`.`ID`.`ID` NOT IN (N, N) ORDER BY `ID`.`ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:fuzz_mut_260`ID`decimal_test`ID`big_decimal` NOT BETWEEN N AND N => fuzz_mut_260`ID`decimal_test`ID`big_decimal`ID`DB`ID`decimal_test`ID`i |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` <= N. => `ID`.`ID`.`ID` <= N ORDER BY `ID`.`ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` <= N ORDER BY `ID`.`ID`.`ID` => `ID`.`ID`.`ID` <= N ORDER BY `ID`.`ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID` <= N ORDER BY `ID`.`ID` => `ID`.`ID` <= N ORDER BY `ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` >= N) AND (`ID`.`ID`.`ID` => (`ID`.`ID`.`ID` >= N) AND (`ID`.`ID`.`ID` <= N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` < N) AND (N => (`ID`.`ID`.`ID` < N) AND (N >= N) ORDER BY `ID`.`ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` <= N) => (`ID`.`ID`.`ID` <= N) OR ((`ID`.`ID`.`ID` <= N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` >= N.) => (`ID`.`ID`.`ID` >= N) ORDER BY `ID`.`ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` <= N) OR (`ID`.`ID`.`ID` => (`ID`.`ID`.`ID` <= N) OR (`ID`.`ID`.`ID` >= NULL) ORDER |
| 1 | FIXPOINT_MISMATCH | fixpoint:((`ID`.`ID`.`ID` <= N) OR (`ID`.`ID`.`ID` => ((`ID`.`ID`.`ID` <= N) OR (`ID`.`ID`.`ID` <= N)) |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` < N) OR (`ID`.`ID`.`ID` => (`ID`.`ID`.`ID` < N) OR (`ID`.`ID`.`ID` >= N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:((`ID`.`ID`.`ID` = N.) => ((`ID`.`ID`.`ID` = N) OR (`ID`.`ID`.`ID` = -N)) |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID`.`ID` BETWEEN N. => (`ID`.`ID`.`ID` BETWEEN N AND N) ORDER BY `ID`.`ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:(`ID`.`ID` BETWEEN N. => (`ID`.`ID` BETWEEN N AND N) ORDER BY `ID`.`ID` ASC |
| 1 | FIXPOINT_MISMATCH | fixpoint:((json_length(CAST(N. => ((json_length(CAST(N AS JSON))), N)) ORDER BY |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT count(-N) => SELECT count(-N) AS `ID` FROM `ID`.`ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID` >= N => `ID`.`ID`.`ID` >= N ORDER BY `ID`.`ID`.`ID` ASC, |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT avg(N * ((round(N * N, N)) => SELECT avg(N * ((round(N * N, N)) / (round(N, N)))) OVER () AS |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * ((round((json_length(CAST(N => SELECT N * ((round((json_length(CAST(N AS JSON))) * N, N)) / (round(N, |
| 1 | FIXPOINT_MISMATCH | fixpoint:AS JSON))) * N) AS DECIMAL32(N,N)) AS `CAST(((json_length(CAST(c_d32 A => AS JSON))) * N) AS DECIMAL32(N,N)) AS `CAST(((json_length(CAST(c_d32 A |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST((N * N) AS DECIMAL64(N,N)) AS `ID` => SELECT CAST((N * N) AS DECIMAL64(N,N)) AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:AST((`ID`.`ID`.`ID` * N) AS DECIMAL64(N,N)) AS `ID` => AST((`ID`.`ID`.`ID` * N) AS DECIMAL64(N,N)) AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * (N => SELECT N * (N * N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * 'S' AS `N => SELECT N * 'S' AS `N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT DISTINCT N * N => SELECT DISTINCT N * N AS `N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * N => SELECT N * N AS `N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT N * (CAST(N => SELECT N * (CAST(N AS DOUBLE)) AS `N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT -N * N AS `ID` => SELECT -N * N AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST((CAST(('S') AS SMALLINT)) AS DOUBLE) AS `CAST((CAST((typeo => SELECT CAST((CAST('S' AS SMALLINT)) AS DOUBLE) AS `CAST((CAST((typeof( |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS ARRAY<JSON>) AS `CAST((typeof(greatest(date('S'), => SELECT CAST('S' AS ARRAY<JSON>) AS `CAST((typeof(greatest(date('S'), |
| 1 | FIXPOINT_MISMATCH | fixpoint:json_length(CAST(('S') AS JSON)) AS `json_length(CAST((typeof(coalesce => json_length(CAST('S' AS JSON)) AS `json_length(CAST((typeof(coalesce(N |
| 1 | FIXPOINT_MISMATCH | fixpoint:json_length(CAST(('S') AS JSON)) AS `json_length(CAST((typeof(greatest => json_length(CAST('S' AS JSON)) AS `json_length(CAST((typeof(greatest(d |
| 1 | FIXPOINT_MISMATCH | fixpoint:json_length(CAST(('S') AS JSON)) AS `json_length(CAST((typeof(greatest => json_length(CAST('S' AS JSON)) AS `json_length(CAST((typeof(greatest(C |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS JSON)->'S' AS `CAST((typeof(CAST(N AS TINYINT)))  => SELECT CAST('S' AS JSON)->'S' AS `CAST((typeof(CAST(N AS TINYINT))) AS |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS JSON)->'S' AS `CAST((typeof(CAST(N AS SMALLINT))) => SELECT CAST('S' AS JSON)->'S' AS `CAST((typeof(CAST(N AS SMALLINT))) |
| 1 | FIXPOINT_MISMATCH | fixpoint:json_length(CAST(('S') AS JSON)) AS `json_length(CAST((typeof(CAST(N A => json_length(CAST('S' AS JSON)) AS `json_length(CAST((typeof(CAST(N AS  |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') => SELECT CAST('S' |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS ARRAY<ARRAY<INT>>) AS `CAST((typeof(parse_json('{ => SELECT CAST('S' AS ARRAY<ARRAY<INT>>) AS `CAST((typeof(parse_json('{"a |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS JSON)->'S' AS `ID` => SELECT CAST('S' AS JSON)->'S' AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT CAST(('S') AS FLOAT) AS `ID` => SELECT CAST('S' AS FLOAT) AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:[SKEW\|`ID`.`ID`.`ID`(N,N,N.)] (SELECT `ID`.`ID`.`ID`, `ID`.`ID`.`ID`, => [SKEW\|`ID`.`ID`.`ID`(N,N,N)] (SELECT `ID`.`ID`.`ID`, `ID`.`ID`.`ID`, |
| 1 | FIXPOINT_MISMATCH | fixpoint:lag(`ID`.`ID`, N, N.) OVER () AS `ID` FROM `ID` AS common_duplicate => lag(`ID`.`ID`, N, N) OVER () AS `ID` FROM `ID` AS common_duplicate |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` ORDER BY count(DISTINCT `ID`.`ID`.`ID`) DESC => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` ORDER BY count(DISTINCT `ID`.`ID`.`ID`) |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` LIMIT N => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` LIMIT N |
| 1 | FIXPOINT_MISMATCH | fixpoint:`ID`.`ID`.`ID`, `ID` HAVING (count(*)) > N => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` HAVING (count(*)) > N |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT reverse(N) AS `ID` => SELECT reverse(N) AS `ID` |
| 1 | FIXPOINT_MISMATCH | fixpoint:TRUE)) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((get_varia => TRUE)) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((get_varia |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S')) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((get_varian => 'S')) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((get_varian |
| 1 | FIXPOINT_MISMATCH | fixpoint:FLOAT)) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((CAST('S' => FLOAT)) - N)) < N) THEN 'S' ELSE 'S' END AS `ID`, CASE WHEN ((CAST('S' |

## Non-defect outcomes (details for the 20 largest)

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID`, `ID` => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` (x52)
- mutant: SELECT `srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`s2`, count(DISTINCT `srfuzz_mut_1078`.`sc2`.`s2`) AS `count(DISTINCT s2)` FROM `srfuzz_mut_1078`.`sc2` GROUP BY `srfuzz_mut_1078`.`sc2`.`s2`, `s2`
- detail: s1 ...`srfuzz_mut_1078`.`sc2`.`s2`, `s2`...  |  s2 ...`srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`s2`...

### FIXPOINT_MISMATCH — fixpoint:BY `ID`.`ID`, `ID` => BY `ID`.`ID`, `ID`.`ID` (x9)
- mutant: SELECT `sc2`.`s2`, `sc2`.`s2`, count(DISTINCT `sc2`.`s2`) AS `count(DISTINCT s2)` FROM (SELECT `srfuzz_mut_1078`.`sc2`.`v1`, `srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`array1`, `srfuzz_mut_1078`.`sc2`.`array2`, `srfuzz_mut_1078`.`sc2`.`array3`, `srfuzz_mut_1078`.`sc2`.`map1`, `srfuzz_mut ...
- detail: s1 ...BY `sc2`.`s2`, `s2`...  |  s2 ...BY `sc2`.`s2`, `sc2`.`s2`...

### FIXPOINT_MISMATCH — fixpoint:((((`ID`.`ID`.`ID` * N) + N.) => ((((`ID`.`ID`.`ID` * N) + N) + N) + N) (x5)
- mutant: SELECT CAST(((((((`srfuzz_mut_256`.`powers_of_2`.`power_250` * 16) + 7237005577332262213973186563042994240829374041602535252466099000494570602496.) + 7237005577332262213973186563042994240829374041602535252466099000494570602496.) + 723700557733226221397318656304299424082937404160253525246609900049457 ...
- detail: s1 ...((((`srfuzz_mut_256`.`powers_of_2`.`power_250` * 16) + 7237005577332262213973186563042994240829374041602535252466099000494570602496.)...  |  s2 ...((((`srfuzz_mut_256`.`powers_of_2`.`power_250` * 16) + 7.237005577332262E75) + 7.237005577332262E75) + 7.237005577332262E75)...

### FIXPOINT_MISMATCH — fixpoint:SELECT N * N AS `N => SELECT N * N AS `N (x3)
- mutant: SELECT 12345678901234567890123456789012345678.0 * 1.0 AS `12345678901234567890123456789012345678.0 * 1.0`
- detail: s1 ...SELECT 12345678901234567890123456789012345678.0 * 1.0 AS `12345678901234567890123456789012345678.0...  |  s2 ...SELECT 1.2345678901234568E37 * 1.0 AS `12345678901234567890123456789012345678.0...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID`, `ID` LIMIT N, N => `ID`.`ID`.`ID`, `ID`.`ID`.`ID` LIMIT N, N (x3)
- mutant: SELECT `srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`s2`, count(DISTINCT `srfuzz_mut_1078`.`sc2`.`s2`) AS `count(DISTINCT s2)` FROM `srfuzz_mut_1078`.`sc2` GROUP BY `srfuzz_mut_1078`.`sc2`.`s2`, `s2` LIMIT 1, 5
- detail: s1 ...`srfuzz_mut_1078`.`sc2`.`s2`, `s2` LIMIT 1, 5...  |  s2 ...`srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`s2` LIMIT 1, 5...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`, `ID` => `ID`.`ID`, `ID`.`ID` (x3)
- mutant: SELECT `sc2`.`array1`, `sc2`.`array1`, count(DISTINCT `sc2`.`s2`) AS `count(DISTINCT s2)` FROM (SELECT `srfuzz_mut_1078`.`sc2`.`v1`, `srfuzz_mut_1078`.`sc2`.`s2`, `srfuzz_mut_1078`.`sc2`.`array1`, `srfuzz_mut_1078`.`sc2`.`array2`, `srfuzz_mut_1078`.`sc2`.`array3`, `srfuzz_mut_1078`.`sc2`.`map1`, `sr ...
- detail: s1 ...`sc2`.`array1`, `array1`...  |  s2 ...`sc2`.`array1`, `sc2`.`array1`...

### FIXPOINT_MISMATCH — fixpoint:lag(`ID`.`ID`.`ID`, N, -N.) OVER (ORDER BY `ID`.`ID`.`ID` ASC ) AS `ID => lag(`ID`.`ID`.`ID`, N, -N) OVER (ORDER BY `ID`.`ID`.`ID` ASC ) AS `ID` (x3)
- mutant: SELECT sum(`a`.`wv`) AS `sum(wv)`, avg(`a`.`wv`) AS `avg(a.wv)`, min(`a`.`wv`) AS `min(wv)`, max(`a`.`wv`) AS `max(wv)` FROM (SELECT lag(`srfuzz_mut_1300`.`t1`.`v2`, 5, -1.) OVER (ORDER BY `srfuzz_mut_1300`.`t1`.`v5` ASC ) AS `wv` FROM `srfuzz_mut_1300`.`t1`) `a`
- detail: s1 ...lag(`srfuzz_mut_1300`.`t1`.`v2`, 5, -1.) OVER (ORDER BY `srfuzz_mut_1300`.`t1`.`v5` ASC ) AS `wv` FROM `srfuzz_mut_1300`.`t1`)...  |  s2 ...lag(`srfuzz_mut_1300`.`t1`.`v2`, 5, -1.0) OVER (ORDER BY `srfuzz_mut_1300`.`t1`.`v5` ASC ) AS `wv` FROM...

### FIXPOINT_MISMATCH — fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) (x2)
- mutant: SELECT ((abs(`result`.`co` - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532) < 0.00001`, `result`.`total` = 4 AS `total = 4` FROM (SELECT corr(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) AS `co`, count(DISTINCT `srfuzz_ ...
- detail: s1 ...0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...  |  s2 ...0.9988445981121532) < 1.0E-5 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...

### FIXPOINT_MISMATCH — fixpoint:((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID => ((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID (x2)
- mutant: SELECT ((abs(`result`.`co` - 80)) / 80) < 0.00001 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) AS `co` FROM `srfuzz_mut_61`.`aggtest` LIMIT 1) `result`
- detail: s1 ...((abs(`result`.`co` - 80)) / 80) < 0.00001 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`srfuzz_mut_61`.`aggtest`.`k`,...  |  s2 ...((abs(`result`.`co` - 80)) / 80) < 1.0E-5 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`srfuzz_mut_61`.`aggtest`.`k`,...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID` >= N => `ID`.`ID`.`ID` >= N ORDER BY `ID`.`ID`.`ID` ASC (x2)
- mutant: SELECT `srfuzz_mut_260`.`decimal_test`.`id`, `srfuzz_mut_260`.`decimal_test`.`big_decimal`, `srfuzz_mut_260`.`decimal_test`.`huge_decimal`, `srfuzz_mut_260`.`decimal_test`.`max_decimal` FROM `srfuzz_mut_260`.`decimal_test` WHERE `srfuzz_mut_260`.`decimal_test`.`id` >= 9999999999999999999999999999999 ...
- detail: s1 ...`srfuzz_mut_260`.`decimal_test`.`id` >= 99999999999999999999999999999999999999999999999999999999.99999999999999999999...  |  s2 ...`srfuzz_mut_260`.`decimal_test`.`id` >= 1.0E56 ORDER BY `srfuzz_mut_260`.`decimal_test`.`id` ASC ...

### FIXPOINT_MISMATCH — fixpoint:(`ID`.`ID`.`ID` = N) => (`ID`.`ID`.`ID` = N) ORDER BY `ID`.`ID`.`ID` (x2)
- mutant: SELECT `srfuzz_mut_260`.`decimal_test`.`id`, `srfuzz_mut_260`.`decimal_test`.`big_decimal`, `srfuzz_mut_260`.`decimal_test`.`huge_decimal`, `srfuzz_mut_260`.`decimal_test`.`max_decimal` FROM `srfuzz_mut_260`.`decimal_test` WHERE (`srfuzz_mut_260`.`decimal_test`.`huge_decimal` IS NULL) OR (`srfuzz_mu ...
- detail: s1 ...(`srfuzz_mut_260`.`decimal_test`.`huge_decimal` = 12345678901234567890123456789012345678901234567890123456.12345678901234567890)...  |  s2 ...(`srfuzz_mut_260`.`decimal_test`.`huge_decimal` = 1.2345678901234567E55) ORDER BY `srfuzz_mut_260`.`decimal_test`.`id`...

### FIXPOINT_MISMATCH — fixpoint:(`ID`.`ID`.`ID` >= N)) AND ((`ID`.`ID`.`huge_decimal => (`ID`.`ID`.`ID` >= N)) AND ((`ID`.`ID`.`ID` IS NOT (x2)
- mutant: SELECT `srfuzz_mut_260`.`decimal_test`.`id`, `srfuzz_mut_260`.`decimal_test`.`big_decimal`, `srfuzz_mut_260`.`decimal_test`.`huge_decimal`, `srfuzz_mut_260`.`decimal_test`.`max_decimal` FROM `srfuzz_mut_260`.`decimal_test` WHERE ((`srfuzz_mut_260`.`decimal_test`.`big_decimal` IS NULL) OR (`srfuzz_mu ...
- detail: s1 ...(`srfuzz_mut_260`.`decimal_test`.`big_decimal` >= 50000000000000000000000000000000000.000000000000000)) AND ((`srfuzz_mut_260`.`decimal_test`.`huge_decimal...  |  s2 ...(`srfuzz_mut_260`.`decimal_test`.`big_decimal` >= 5.0E34)) AND ((`srfuzz_mut_260`.`decimal_test`.`huge_decimal` IS NOT...

### FIXPOINT_MISMATCH — fixpoint:N AS `ID` FROM `ID`.`ID` => N AS `ID` FROM `ID`.`ID` (x2)
- mutant: SELECT `srfuzz_mut_265`.`t_decimal_overflow`.`c_id` - 1.12345678901234567890 AS `c_id - 1.12345678901234567890` FROM `srfuzz_mut_265`.`t_decimal_overflow` WHERE 1 = 1
- detail: s1 ...1.12345678901234567890 AS `c_id - 1.12345678901234567890` FROM `srfuzz_mut_265`.`t_decimal_overflow`...  |  s2 ...1.1234567890123457 AS `c_id - 1.12345678901234567890` FROM `srfuzz_mut_265`.`t_decimal_overflow`...

### FIXPOINT_MISMATCH — fixpoint:CAST(N AS DECIMAL(N,N)) AS `ID`, CAST(N AS DECIMAL64(N,N)) => CAST(N AS DECIMAL64(N,N)) AS `ID`, CAST(N AS DECIMAL64(N,N)) (x2)
- mutant: SELECT 1 AS `1`, -1 AS `-1`, 1.23456 AS `1.23456`, CAST(1.123 AS FLOAT) AS `CAST(1.123 AS FLOAT)`, CAST(1.123 AS DOUBLE) AS `CAST(1.123 AS DOUBLE)`, CAST(10 AS BIGINT) AS `CAST(10 AS BIGINT)`, CAST(100 AS LARGEINT) AS `CAST(100 AS LARGEINT)`, 1000000000000 AS `1000000000000`, 1 + 1 AS `1 + 1`, 100 * ...
- detail: s1 ...CAST(1.123000000 AS DECIMAL(9,0)) AS `CAST(1.123000000 AS DECIMAL(9,0))`, CAST(1.123 AS DECIMAL64(10,7))...  |  s2 ...CAST(1.123000000 AS DECIMAL64(9,0)) AS `CAST(1.123000000 AS DECIMAL(9,0))`, CAST(1.123 AS DECIMAL64(10,7))...

### FIXPOINT_MISMATCH — fixpoint:get_json_string(CAST(('S') AS JSON), 'S') AS `get_json_string(CAST((ty => get_json_string(CAST('S' AS JSON), 'S') AS `get_json_string(CAST((type (x2)
- mutant: SELECT get_json_string(CAST(('varchar') AS JSON), '$.a') AS `get_json_string(CAST((typeof(CAST(1 AS VARCHAR))) AS JSON), '$.a')`
- detail: s1 ...get_json_string(CAST(('varchar') AS JSON), '$.a') AS `get_json_string(CAST((typeof(CAST(1...  |  s2 ...get_json_string(CAST('varchar' AS JSON), '$.a') AS `get_json_string(CAST((typeof(CAST(1...

### FIXPOINT_MISMATCH — fixpoint:lag(`ID`.`ID`.`ID`, N, -N.) OVER (PARTITION BY `ID`.`ID`.`ID` ORDER BY => lag(`ID`.`ID`.`ID`, N, -N) OVER (PARTITION BY `ID`.`ID`.`ID` ORDER BY  (x2)
- mutant: SELECT sum(`a`.`wv`) AS `sum(wv)`, avg(`a`.`wv`) AS `avg(wv)`, min(`a`.`wv`) AS `min(wv)`, max(`a`.`wv`) AS `max(wv)` FROM (SELECT lag(`srfuzz_mut_1300`.`t1`.`v3`, 5, -1.) OVER (PARTITION BY `srfuzz_mut_1300`.`t1`.`v1` ORDER BY `srfuzz_mut_1300`.`t1`.`v5` ASC ) AS `wv` FROM `srfuzz_mut_1300`.`t1`) ` ...
- detail: s1 ...lag(`srfuzz_mut_1300`.`t1`.`v3`, 5, -1.) OVER (PARTITION BY `srfuzz_mut_1300`.`t1`.`v1` ORDER BY `srfuzz_mut_1300`.`t1`.`v5`...  |  s2 ...lag(`srfuzz_mut_1300`.`t1`.`v3`, 5, -1.0) OVER (PARTITION BY `srfuzz_mut_1300`.`t1`.`v1` ORDER BY `srfuzz_mut_1300`.`t1`.`v5`...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID`)[N.] AS `ID` FROM `ID`.`ID` => `ID`.`ID`.`ID`)[N.] AS `ID` FROM `ID`.`ID` (x1)
- mutant: SELECT map_agg(CAST(`srfuzz_mut_54`.`t1`.`c1` AS DECIMAL128(27,9)), `srfuzz_mut_54`.`t1`.`c3`)[2.] AS `map_agg(CAST(c1 AS DECIMAL128(27,9)), c3)[2]` FROM `srfuzz_mut_54`.`t1`
- detail: s1 ...`srfuzz_mut_54`.`t1`.`c3`)[2.] AS `map_agg(CAST(c1 AS DECIMAL128(27,9)), c3)[2]` FROM `srfuzz_mut_54`.`t1`...  |  s2 ...`srfuzz_mut_54`.`t1`.`c3`)[0.] AS `map_agg(CAST(c1 AS DECIMAL128(27,9)), c3)[2]` FROM `srfuzz_mut_54`.`t1`...

### FIXPOINT_MISMATCH — fixpoint:- N)) / N) < N AS `ID`, `ID`.`ID` => - N)) / N) < N AS `ID`, `ID`.`ID` = (x1)
- mutant: SELECT ((abs(`result`.`co` - 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total` = 4 AS `total = 4` FROM (SELECT covar_samp(`aggtest`.`k`, `aggtest`.`v`) AS `co`, count(DISTINCT `aggtest`.`k`) AS `total` FROM (SELECT `srfuzz_mut_61`.`aggtest`.`no`, `srfuzz_mut_61`.`aggtes ...
- detail: s1 ...- 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total`...  |  s2 ...- 120)) / 120) < 1.0E-5 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total` =...

### FIXPOINT_MISMATCH — fixpoint:`ID`.`ID`.`ID`, array_agg(N) OVER (PARTITION BY `ID`.`ID`.`ID` => `ID`.`ID`.`ID`, array_agg(N) OVER (PARTITION BY `ID`.`ID`.`ID` (x1)
- mutant: SELECT sum(round(array_sum(array_map(x -> murmur_hash3_32(coalesce(`__LAMBDA_TABLE`.`__LAMBDA_TABLE`.`x`, 0)), `t`.`arr_basic`)), 0)) AS `fingerprint` FROM (SELECT `srfuzz_mut_114`.`t0`.`v1`, `srfuzz_mut_114`.`t0`.`v2`, `srfuzz_mut_114`.`t0`.`v3`, array_agg(123456789012345678901234567890.123456789)  ...
- detail: s1 ...`srfuzz_mut_114`.`t0`.`v3`, array_agg(123456789012345678901234567890.123456789) OVER (PARTITION BY `srfuzz_mut_114`.`t0`.`v1`...  |  s2 ...`srfuzz_mut_114`.`t0`.`v3`, array_agg(1.2345678901234568E29) OVER (PARTITION BY `srfuzz_mut_114`.`t0`.`v1`...

### FIXPOINT_MISMATCH — fixpoint:fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a => fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a (x1)
- mutant: SELECT `srfuzz_mut_115`.`test_array_contains`.`id` FROM `srfuzz_mut_115`.`test_array_contains` WHERE array_contains(`srfuzz_mut_115`.`test_array_contains`.`array_decimal64`, 12345678.90) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id` ASC
- detail: s1 ...fuzz_mut_115`.`test_array_contains`.`array_decimal64`, 12345678.90) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...  |  s2 ...fuzz_mut_115`.`test_array_contains`.`array_decimal64`, 1.23456789E7) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...
