# AST Mutation Fuzz Report (P1)

seeds: 4330, mutants: 73607, dropped as grammar-unreachable: 10027 (12.0%)

## Dropped mutants (404 distinct reasons)

| count | reason | sample |
|---:|---|---|
| 9152 | deparse-threw:NullPointerException@com.starrocks.sql.analyzer.AnalyzerUtils#replaceNullType2Boolean | `<unrenderable>` |
| 149 | reparse-failed:ParsingException: Getting syntax error at line 2, column 34. Detail message: Unexpected input '{', the most similar input is {')'}. | `SELECT sum_map(`m`)[1], sum_map(`m`)[2], sum_map(`m`)[3] FROM (SELECT PseudoType.AnyMapType{1:10,2:20} AS `m` UNION ALL SELECT NULL AS `m` UNION ALL SELECT PseudoType.AnyMapType{0.0:5,3:30} AS `m`) `t`` |
| 57 | reparse-failed:ParsingException: Getting syntax error at line 1, column 28. Detail message: Unexpected input '{', the most similar input is {<EOF>, ';'}. | `SELECT PseudoType.AnyMapType{NULL:1} LIMIT 7, 2` |
| 49 | reparse-failed:ParsingException: Getting syntax error at line 1, column 62. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id`, array_position(`array_map`, PseudoType.AnyMapType{'a':1,'b':2}) FROM `test_array_contains_complex_type` GROUP BY `id` ORDER BY `id` ASC` |
| 40 | reparse-failed:ParsingException: Getting syntax error at line 3, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM `test_array_contains` WHERE array_contains(`array_int`, PseudoType.AnyMapType{}) ORDER BY `id` ASC` |
| 17 | reparse-failed:ParsingException: Getting syntax error at line 1, column 47. Detail message: Unexpected input '{', the most similar input is {'ORDER', ')'}. | `SELECT array_agg(DISTINCT PseudoType.AnyMapType{}) FROM `ss` ORDER BY 1 ASC` |
| 13 | reparse-failed:ParsingException: Getting syntax error at line 1, column 38. Detail message: No viable statement for input 'array_agg(PseudoType.AnyMapType{'. | `SELECT array_agg(PseudoType.AnyMapType{2:3}) FROM `ss` WHERE `srfuzz_mut_37`.`test_array_agg`.`col_boolean`` |
| 13 | reparse-failed:ParsingException: Getting syntax error at line 5, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM (SELECT * FROM `test_array_contains_complex_type` INTERSECT SELECT * FROM `test_array_contains_complex_type`) `test_array_contains_complex_type` WHERE array_contains(`array_map`, PseudoType.AnyMapType{'a':1,'b':2}) ORDER BY `id` ASC` |
| 10 | reparse-failed:ParsingException: Getting syntax error at line 2, column 43. Detail message: Unexpected input '{', the most similar input is {')'}. | `SELECT sum_map(`m`)[1], sum_map(`m`)[2] FROM (SELECT DISTINCT PseudoType.AnyMapType{1:10,2:20} AS `m`) `t`` |
| 10 | reparse-failed:ParsingException: Getting syntax error at line 1, column 21. Detail message: Unexpected input '(', the most similar input is {<EOF>, ';'}. | `SELECT array_generate(1, 9.`c6`)` |
| 7 | reparse-failed:ParsingException: Getting syntax error at line 1, column 38. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT array_sum(PseudoType.AnyMapType{}) FROM `array_test` ORDER BY `pk` ASC` |
| 6 | reparse-failed:ParsingException: Getting syntax error at line 1, column 19. Detail message: Unexpected input '(', the most similar input is {<EOF>, ';'}. | `SELECT group_concat(1.`value`,2 SEPARATOR ',') FROM `ss` GROUP BY `id` ORDER BY 1 ASC` |
| 6 | reparse-failed:ParsingException: Getting syntax error at line 3, column 43. Detail message: No viable statement for input '(last_day(`dt`, PseudoType.AnyMapType{'. | `SELECT count(*) FROM `t1` WHERE (last_day(`dt`, PseudoType.AnyMapType{})) BETWEEN '2025-01-01' AND '2025-09-28'` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 53. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT approx_top_k(`c_bigint`, PseudoType.AnyMapType{}) FROM `t_without_null`` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 66. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `c_partition`, approx_top_k(`c_bool`, PseudoType.AnyMapType{}, 100) FROM `t_bool` GROUP BY `c_partition` ORDER BY `c_partition` ASC` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 47. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT min_n(`c_tinyint`, PseudoType.AnyMapType{}) FROM `t_without_null`` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 48. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT min_n(`c_datetime`, PseudoType.AnyMapType{}) FROM `t_with_null`` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 34. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT sleep(PseudoType.AnyMapType{})` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 3, column 43. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT count(*) FROM `_statistics_`.`column_statistics` WHERE `table_name` = (PseudoType.AnyMapType{})` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 71. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT array_generate('2025-10-01', '2027-10-05', PseudoType.AnyMapType{}, 'year')` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 4, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM (SELECT * FROM `test_array_contains_complex_type`) `test_array_contains_complex_type` , generate_series(1,3) srfuzz_n1(`e`)  WHERE array_contains(`array_map`, PseudoType.AnyMapType{'c':3,'d':4}) ORDER BY `id` ASC` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 43. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT array_generate(PseudoType.AnyMapType{}, 2, 1)` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 45. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT base64_to_bitmap(PseudoType.AnyMapType{})` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 34. Detail message: No viable statement for input 'CAST((PseudoType.AnyMapType{'. | `SELECT CAST((PseudoType.AnyMapType{}) AS SMALLINT)` |
| 3 | reparse-failed:ParsingException: Getting syntax error at line 3, column 22. Detail message: Unexpected input '[', the most similar input is {<EOF>, ';'}. | `SELECT round(var_samp(`val1`), 3) FROM `t1` WHERE `k` IN (2, 5, 6)['k']` |

| Outcome | Count |
|---|---:|
| OK | 41163 |
| ANALYZE_REJECTED | 36749 |
| ANALYZE_INTERNAL_ERROR | 19 |
| DEPARSE_THROW | 0 |
| REPARSE_FAIL | 0 |
| REANALYZE_FAIL | 0 |
| FIXPOINT_MISMATCH | 6 |

## Bug candidates (1 distinct signatures)

### ANALYZE_INTERNAL_ERROR — UnsupportedException@com.starrocks.sql.common.UnsupportedException#unsupportedException (x19)

- detail: UnsupportedException: Table function cannot appear on the left side of a join. Place it on the right side (optionally with LATERAL) or wrap it with TABLE(...).
- seed:     with w_empty as (select * from t1 where c1 = 'not-exist'), w1 as (select k1, k2, count(1) as cnt from t1 group by k1, k2), w2 as (select k1, k2, c1, count(1) as cnt from t1 group by k1, k2, c1) select count(1) from w_empty tt1 join t1 tt2 join t1 tt3 join w1 tt4 join w2 tt5
- mutation: M6-nesting TABLE_FUNCTION at JoinRelation.left: `w_empty` AS `tt1` -> (SELECT * FROM `w_empty` AS `tt1`) `tt1`, generate_series(1, 3) `srfuzz_n1`(`e`)
- mutant:   WITH `w_empty` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, `srfuzz_mut_7`.`t1`.`c1` FROM `srfuzz_mut_7`.`t1` WHERE `srfuzz_mut_7`.`t1`.`c1` = 'not-exist') , `w1` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, count(1) AS `cnt` FROM `srfuzz_mut_7`.`t1` GROUP BY `srf ...

## Analyzer rejections (4107 signatures, 36749 instances)

### By operator

| operator | instances | share | signatures | most common reason |
|---|---:|---:|---:|---|
| M1-M4-expr | 20674 | 56.3% | 2539 | Column 'S' cannot be resolved. |
| M7-typestress | 9133 | 24.9% | 1360 | Column 'S' cannot be resolved. |
| M5-clause | 3825 | 10.4% | 177 | Column 'S' cannot be resolved. |
| M6-nesting | 3117 | 8.5% | 31 | Column 'S' cannot be resolved. |

### Highest volume (40 of 4107 signatures)

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 8510 | M1-M4-expr | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | BinaryPredicate[0]: `k2` -> `table_database` |
| 2085 | M1-M4-expr | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | CompoundPredicate[0]: `k1` = -1 -> (`srfuzz_mut_1`.`target_table`.`k1` = -1) AND (`srfuzz_mut_1`.`target_table`.`event_day` = '2021-01-01') |
| 1827 | M1-M4-expr | SemanticException@AggregationAnalyzer$VerifyExpressionVisitor#visitFunctionCall | Unsupported nest aggregation function inside aggregation. | FunctionCallExpr[0]: 1 -> count(1) |
| 1082 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M6-nesting ASOF_JOIN at SelectRelation.from: `t3_predicate` -> (SELECT * FROM `t3_predicate`) `t3_predicate` ASOF LEFT JOIN (SELECT * FROM `t3_predicate`) `srfuzz_n1` ON `t3_predicate`.`k2` = `srfuzz_n1`.`k2` AND `t3_predic ... |
| 878 | M5-clause | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M5-clause select#2: add WHERE `tt1`.`k1` = `srfuzz_mut_7`.`tt2`.`k1` |
| 876 | M5-clause | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M5-clause select#0: add GROUP BY `k1` \| M9-session: sql_mode=131104 |
| 852 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | M6-nesting SELF_JOIN at SelectRelation.from: `t3_predicate` -> (SELECT * FROM `t3_predicate`) `t3_predicate` RIGHT OUTER JOIN (SELECT * FROM `t3_predicate`) `srfuzz_n1` ON `t3_predicate`.`k2` > `srfuzz_n1`.`k2` |
| 839 | M6-nesting | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M6-nesting UNION_DISTINCT at SelectRelation.from: `target_table` -> (SELECT * FROM `target_table` UNION SELECT * FROM `target_table`) `target_table` |
| 723 | M7-typestress | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M7-typestress STRUCT_FIELD at SelectListItem[1]: `column_name` -> (`column_name`).`k2` \|\| marker: `column_name`.`k2` |
| 698 | M5-clause | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | M5-clause select#2: remove GROUP BY `k1`, `k2`, `c1` |
| 615 | M5-clause | SemanticException@SelectAnalyzer#analyze | cannot combine SELECT DISTINCT with aggregate functions without GROUP BY. | M5-clause select#3: add DISTINCT |
| 583 | M7-typestress | SemanticException@QueryAnalyzer#resolveTable | Unknown table 'S'. | M7-typestress CAST_CHAIN at CompoundPredicate[0]: `k1` = -1 -> CAST(CAST(CAST((`k1` = -1) AS FLOAT) AS ARRAY<INT>) AS DATETIME) \|\| marker: CAST((CAST((CAST((`k1` = -1) AS FLOAT)) AS ARRAY<INT>)) AS DATETIME) |
| 499 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type TINYINT because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at WhereClause: `k2` = 1 -> (`k2` = 1)[0] \|\| marker: `k2` = 1[0] |
| 483 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BIGINT because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[2]: count(1) -> (count(1))['0'] \|\| marker: count(1)['0'] |
| 422 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at BinaryPredicate[1]: 'not-exist' -> ('not-exist')[1] \|\| marker: 'not-exist'[1] |
| 414 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type INT because it is not an array or a map or a struct. | M7-typestress MAP_KEY at BinaryPredicate[0]: `tt1`.`k1` -> (`tt1`.`k1`)[CAST(NULL AS VARCHAR)] \|\| marker: `tt1`.`k1`[CAST(NULL AS VARCHAR)] |
| 274 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | array subscript must have type integer. | M7-typestress MAP_KEY at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> (array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC))['0'] \|\| marker: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)['0'] |
| 232 | M1-M4-expr | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | FunctionCallExpr[0]: sum(murmur_hash3_32(`__col_0`)) -> lower(murmur_hash3_32(`__col_0`)) |
| 230 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedFunction | Table function cannot be used in expression. | M7-typestress JSON_PATH at SelectListItem[4]: `c5` -> json_each(CAST((`c5`) AS JSON)) \|\| marker: json_each(CAST(`c5` AS JSON)) |
| 222 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | GROUP BY clause cannot contain aggregations. | M5-clause select#0: add GROUP BY approx_top_k(`c_largeint`) |
| 182 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DOUBLE because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[3]: avg(`c1`) -> (avg(`c1`))[1] \|\| marker: avg(`c1`)[1] \| M9-session: enable_groupby_use_output_alias=true |
| 179 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR(N) because it is not an array or a map or a struct. | M7-typestress MAP_KEY at BinaryPredicate[0]: `c1` -> (`c1`)[''] \|\| marker: `c1`[''] |
| 175 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | 'S' must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at BinaryPredicate[1]: 'not-exist' -> ('not-exist').b \|\| marker: 'not-exist'.`b` |
| 148 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | M5-clause select#0: add WHERE count(*) |
| 144 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | group_concat distinct should use constant separator. | FunctionCallExpr[1]: ',' -> group_concat(CAST(`srfuzz_mut_19`.`skew_agg`.`c3` AS VARCHAR) SEPARATOR ',') |
| 134 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_contains should be an array. | FunctionCallExpr[0]: <ArrayExpr> -> 'a' |
| 132 | M1-M4-expr | SemanticException@SemanticException#appendOnlyOnceMsg | Column 'S' cannot be resolved in array_map(x -> murmur_hash3_32(coalesce(x, N)), `ID`.`ID`). | ArithmeticExpr[0]: coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(`x`, 0)), `arr_basic`)), 0) -> coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(`__LAMBDA_TABLE`.`__LAMBDA_TABLE`.`x`, 0)), `t`.`arr_int`)), 0) |
| 131 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_slice should be an array. | M7-typestress COLLECTION_FN at SelectListItem[2]: `usage` -> array_slice((`usage`), 1, 3) \|\| marker: array_slice(`usage`, 1, 3) |
| 120 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_position should be an array. | FunctionCallExpr[0]: <ArrayExpr> -> 6000 |
| 95 | M1-M4-expr | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | BinaryPredicate[1]: 'not-exist' -> count(1) |
| 91 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | percentile_cont 's second parameter should be constant and its type should be numeric. | FunctionCallExpr[1]: 0.5 -> count(DISTINCT `srfuzz_mut_21`.`t0`.`v1`) |
| 89 | M1-M4-expr | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | BinaryPredicate[0]: `tt1`.`k1` -> `k2` |
| 85 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | percentile_disc_lc 's second parameter should be constant and its type should be numeric. | FunctionCallExpr[1]: 0.5 -> percentile_disc_lc(`srfuzz_mut_21`.`t0`.`v1`, 0.5) |
| 85 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BOOLEAN because it is not an array or a map or a struct. | M7-typestress MAP_KEY at WhereClause: (`c_id` >= 71) AND (`c_id` <= 90) -> ((`c_id` >= 71) AND (`c_id` <= 90))['k'] \|\| marker: (`c_id` >= 71) AND (`c_id` <= 90)['k'] |
| 82 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | N-th input of arrays_overlap should be an array, rather than varchar. | FunctionCallExpr[0]: `s_1` -> 'd' \| M9-session: enable_groupby_use_output_alias=true |
| 73 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type NULL_TYPE because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: `column_0` -> (`column_0`)['k'] \|\| marker: `column_0`['k'] |
| 68 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATE because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[5]: `c6` -> (`c6`)[''] \|\| marker: `c6`[''] |
| 67 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATETIME because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: `c7` -> (`c7`)[1] \|\| marker: `c7`[1] |
| 66 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The only one input of array_min should be an array, rather than int(N). | FunctionCallExpr[0]: `s_1` -> array_length(`srfuzz_mut_118`.`array_test`.`aad_1`) |
| 64 | M1-M4-expr | SemanticException@SemanticException#appendOnlyOnceMsg | Column 'S' cannot be resolved in array_map(x -> murmur_hash3_32(coalesce(`ID`, N)), `ID`). | FunctionCallExpr[1]: `arr_distinct` -> `arr_order_by_float` |

4067 further signatures (12195 instances) below this cut.

### Rare signatures, count <= 2 (3023 of them, 3631 instances)

A false rejection would be here rather than above: one the analyzer should not have
issued is by nature uncommon. Read the message against the mutation next to it and ask
whether the mutant really was invalid.

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 1 | M5-clause | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | M5-clause select#3: add GROUP BY `c1` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The only one input of array_distinct should be an array, rather than smallint(N). | M7-typestress COLLECTION_FN at SelectListItem[0]: `c2` -> array_distinct((`c2`)) \|\| marker: array_distinct(`c2`) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: sum(array<json>). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c1` -> CAST(CAST((`c1`) AS STRING) AS ARRAY<JSON>) \|\| marker: CAST((CAST(`c1` AS VARCHAR(65533))) AS ARRAY<JSON>) \| M9-session: sql_mode=34 |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from float to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c3` -> CAST(CAST(CAST((`c3`) AS FLOAT) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) AS ARRAY<STRUCT<a INT, b VARCHAR(8)>>) \|\| marker: CAST((CAST((CAST(`c3` AS FLOAT)) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>)) AS ARRAY<struct<`a` int(11), `b` varchar(8) ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c5` -> CAST((`c5`) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) \|\| marker: CAST(`c5` AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from varbinary to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c17` -> CAST(CAST(CAST((`c17`) AS VARBINARY) AS VARBINARY) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) \|\| marker: CAST((CAST((CAST(`c17` AS VARBINARY)) AS VARBINARY)) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from smallint(N) to array<array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c2` -> CAST(CAST(CAST((`c2`) AS ARRAY<ARRAY<INT>>) AS VARBINARY) AS LARGEINT) \|\| marker: CAST((CAST((CAST(`c2` AS ARRAY<ARRAY<INT>>)) AS VARBINARY)) AS LARGEINT) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to array<array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c17` -> CAST((`c17`) AS ARRAY<ARRAY<INT>>) \|\| marker: CAST(`c17` AS ARRAY<ARRAY<INT>>) \| M9-session: sql_mode=131104 |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from smallint(N) to struct<`ID` array<int(N)>, `ID` map<int(N),int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c1` -> CAST(CAST(CAST((`c1`) AS SMALLINT) AS STRUCT<a ARRAY<INT>, b MAP<INT, INT>>) AS CHAR(1)) \|\| marker: CAST((CAST((CAST(`c1` AS SMALLINT)) AS struct<`a` array<int(11)>, `b` map<int(11),int(11)>>)) AS CHAR(1)) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to array<json> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[3]: `c5` -> CAST(CAST(CAST((`c5`) AS ARRAY<JSON>) AS MAP<VARCHAR(16), STRUCT<a INT>>) AS JSON) \|\| marker: CAST((CAST((CAST(`c5` AS ARRAY<JSON>)) AS MAP<VARCHAR(16),struct<`a` int(11)>>)) AS JSON) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to struct<`ID` int(N), `ID` varchar(N)> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: 'Test Case 1' -> CAST(CAST(CAST(('Test Case 1') AS LARGEINT) AS STRUCT<a INT, b VARCHAR(8)>) AS ARRAY<STRUCT<a INT, b VARCHAR(8)>>) \|\| marker: CAST((CAST((CAST('Test Case 1' AS LARGEINT)) AS struct<`a` int(11), `b` varchar(8)>)) AS ARRAY<struct<`a` int( ... |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(((((((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAS | FunctionCallExpr[1]: ',' -> 1.0E308 |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from map<varchar(N),map<int(N),array<json>>> to varchar in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: ((((((CAST(`c1` AS VARCHAR(65533))) OR ':') OR (CAST(`c2` AS VARCHAR(65533)))) OR ':') OR (CAST(`c3` AS VARCHAR(65533)))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) -> CAST(CAST((((((((CAST(`c1` AS VARCHAR(65533))) OR ':') OR (CAST(`c2` AS VARCHAR(655 ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<map<varchar(N),int(N)>> to map<int(N),array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: group_concat(((((((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHAR(65533)))) OR ':') OR `c8`) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',') -> CAST(CAST(CAST((group_concat(((((((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST ... |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(((CAST(`ID`.`ID` AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARC | FunctionCallExpr[1]: ',' -> 2147483647 |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: md5(array<varchar>). | FunctionCallExpr[0]: group_concat(((CAST(`c18` AS VARCHAR(65533))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',') -> split(((CAST(`c18` AS VARCHAR(65533))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))), ',') |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCompoundPredicate | CAST((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN)) AS MAP<INT,ARRAY<INT>>) can not be converted to boolean type.. | M7-typestress CAST_CHAIN at CompoundPredicate[0]: (CAST(`c18` AS VARCHAR(65533))) OR ':' -> CAST(((CAST(`c18` AS VARCHAR(65533))) OR ':') AS MAP<INT, ARRAY<INT>>) \|\| marker: CAST(((CAST(`c18` AS VARCHAR(65533))) OR ':') AS MAP<INT,ARRAY<INT>>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from struct<`ID` array<int(N)>, `ID` map<int(N),int(N)>> to json in sql `ID`a`ID`b`ID`. | M7-typestress CAST_CHAIN at CompoundPredicate[0]: (((CAST(`c13` AS VARCHAR(65533))) OR ':') OR (CAST(`c14` AS VARCHAR(65533)))) OR ':' -> CAST(CAST(CAST(((((CAST(`c13` AS VARCHAR(65533))) OR ':') OR (CAST(`c14` AS VARCHAR(65533)))) OR ':') AS STRUCT<a ARRAY<INT>, b MAP<INT, INT>>) AS JSON) AS ARRAY< ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | md5(group_concat(((((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN))) OR (CAST('S | M7-typestress STRUCT_FIELD at SelectListItem[1]: md5(group_concat(((((CAST(`c13` AS VARCHAR(65533))) OR ':') OR (CAST(`c14` AS VARCHAR(65533)))) OR ':') OR (CAST(`cnt` AS VARCHAR(65533))) SEPARATOR ',')) -> (md5(group_concat(((((CAST(`c13` AS VARCHAR(65533))) OR ':') OR (CAST(`c14` AS VARCHAR(65533) ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | md5(group_concat(((((((((((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN))) OR (C | M7-typestress STRUCT_FIELD at SelectListItem[1]: md5(group_concat(((((((((((CAST(`c1` AS VARCHAR(65533))) OR ':') OR (CAST(`c5` AS VARCHAR(65533)))) OR ':') OR (CAST(`c9` AS VARCHAR(65533)))) OR ':') OR (CAST(`c13` AS VARCHAR(65533)))) OR ':') OR (CAST(`c17` AS VARCHAR(65533)))) OR ':') OR (CAST(`cn ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | avg(`ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: avg(`c3`) -> (avg(`c3`)).a \|\| marker: avg(`c3`).`a` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | count(`ID`.`ID`.`ID` + `ID`.`ID`.`ID` + N) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[1]: count((`c3` + `c1`) + 3) -> (count((`c3` + `c1`) + 3)).`c4` \|\| marker: count((`c3` + `c1`) + 3).`c4` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer#getArithmeticFunction | cast type varbinary with type smallint(N) is invalid. | M7-typestress CAST_CHAIN at ArithmeticExpr[0]: `c3` + `c1` -> CAST((`c3` + `c1`) AS VARBINARY) \|\| marker: CAST((`c3` + `c1`) AS VARBINARY) \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: min(array<bigint(N)>). | FunctionCallExpr[0]: `c2` -> array_agg(`srfuzz_mut_19`.`skew_agg`.`c1`) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(DISTINCT date_trunc('S', `ID`.`ID`.`ID`)). | FunctionCallExpr[0]: `c0` -> date_trunc('week', `srfuzz_mut_19`.`skew_agg`.`c4`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: max(array<tinyint(N)>). | FunctionCallExpr[0]: `c2` -> array_agg(DISTINCT 2) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: to_bitmap(array<tinyint(N)>). | FunctionCallExpr[0]: `c0` + `c3` -> array_agg(DISTINCT 2) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: date_trunc(varchar, hll). | FunctionCallExpr[1]: `c4` -> hll_hash(`srfuzz_mut_19`.`skew_agg`.`c5`) |
| 1 | M7-typestress | SemanticException@AggregationAnalyzer#analyze | 'S'week'S'$.a'S' must be an aggregate expression or appear in GROUP BY clause. | M7-typestress JSON_PATH at FunctionCallExpr[1]: `c4` -> get_json_string(CAST((`c4`) AS JSON), '$.a') \|\| marker: get_json_string(CAST(`c4` AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: date_trunc(varchar, json). | M7-typestress JSON_PATH at FunctionCallExpr[1]: `c4` -> CAST((`c4`) AS JSON) -> '$.a' \|\| marker: CAST(`c4` AS JSON)->'$.a' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | ifnull(sum(murmur_hash3_32(`ID`.`ID`)), N) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at ArithmeticExpr[1]: ifnull(sum(murmur_hash3_32(`__col_3`)), 0) -> (ifnull(sum(murmur_hash3_32(`__col_3`)), 0)).`no such field` \|\| marker: ifnull(sum(murmur_hash3_32(`__col_3`)), 0).`no such field` \| M9-session: sql_mode=288 |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: left(double, bitmap). | FunctionCallExpr[1]: 6 -> to_bitmap(`srfuzz_mut_19`.`skew_agg`.`c0` + `srfuzz_mut_19`.`skew_agg`.`c3`) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | length(`ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at FunctionCallExpr[0]: length(`c0`) -> (length(`c0`)).`no such field` \|\| marker: length(`c0`).`no such field` |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(DISTINCT CAST(length(`ID`.`ID`.`ID`) AS JSON)->'S'). | M7-typestress JSON_PATH at FunctionCallExpr[0]: length(`c0`) -> CAST((length(`c0`)) AS JSON) -> 'k' \|\| marker: CAST((length(`c0`)) AS JSON)->'k' \| M9-session: sql_mode=131104 |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<varchar> to varchar in sql `ID`. | CastExpr[0]: `c1` -> split('a,b,c', ',') |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(murmur_hash3_32(`ID`.`ID`)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at FunctionCallExpr[0]: sum(murmur_hash3_32(`__col_2`)) -> (sum(murmur_hash3_32(`__col_2`))).c \|\| marker: sum(murmur_hash3_32(`__col_2`)).`c` \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: char_length(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> char_length(`a1`) |
| 1 | M5-clause | SemanticException@AnalyticAnalyzer#verifyAnalyticExpression | Expressions in the PARTITION BY clause must not be constant: split('S', 'S') (in rank() OVER (PARTITION BY split('S', 'S') ORDER BY 'S' ASC)). | M5-clause select#1: add window over item 1 ('aaa' -> rank() OVER (PARTITION BY split('a,b,c', ',') ORDER BY 'aaa')) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: sqrt(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> sqrt(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: hll_cardinality(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> hll_cardinality(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: year(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> year(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: length(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> length(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: month(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> month(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: unhex(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> unhex(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: floor(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> floor(`a1`) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bigint(N), varchar, array<varchar>, largeint(N)). | FunctionCallExpr[2]: ',' -> split('a,b,c', ',') |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(array<varchar>, varchar). | FunctionCallExpr[0]: `c2` -> split('a,b,c', ',') |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT CAST(`ID`.`ID` AS JSON)->'S', upper(`ID`.`ID`), 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) can't rewrite distinct to group by on (json,v | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c2` -> CAST((`c2`) AS JSON) -> 'k' \|\| marker: CAST(`c2` AS JSON)->'k' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<bigint(N)> to largeint(N) in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> CAST((array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)) AS LARGEINT) \|\| marker: CAST((array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)) AS LARGEINT) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<bigint(N)> to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[1]: array_agg(DISTINCT `c3` ORDER BY `c3` ASC, `c4` ASC) -> CAST((array_agg(DISTINCT `c3` ORDER BY `c3` ASC, `c4` ASC)) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) \|\| marker: CAST((array_agg(DISTINCT `c3` ORDER BY `c3` ASC, `c4` ASC)) AS MAP<VARCHAR(16),MAP ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: upper(array<struct<`ID` int(N), `ID` varchar(N)>>). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c4` -> CAST((`c4`) AS ARRAY<STRUCT<a INT, b VARCHAR(8)>>) \|\| marker: CAST(`c4` AS ARRAY<struct<`a` int(11), `b` varchar(8)>>) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), CAST(CAST('S' AS BOOLEAN) AS DECIMAL128(N,N)) ORDE | M7-typestress CAST_CHAIN at FunctionCallExpr[2]: ',' -> CAST(CAST((',') AS BOOLEAN) AS DECIMAL(27, 9)) \|\| marker: CAST((CAST(',' AS BOOLEAN)) AS DECIMAL128(27,9)) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), CAST('S' AS TIME) ORDER BY abs(`ID`.`ID` + `ID`.`I | M7-typestress CAST_CHAIN at FunctionCallExpr[2]: ',' -> CAST((',') AS TIME) \|\| marker: CAST(',' AS TIME) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | ceil(sum(`ID`.`ID`.`ID`)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[2]: ceil(sum(`c5`)) -> (ceil(sum(`c5`))).`a` \|\| marker: ceil(sum(`c5`)).`a` |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | array_agg(DISTINCT CAST(N AS JSON)->'S') can't rewrite distinct to group by on (json). | M7-typestress JSON_PATH at FunctionCallExpr[0]: 2 -> CAST((2) AS JSON) -> '$.a' \|\| marker: CAST(2 AS JSON)->'$.a' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from TIME to struct<`ID` array<int(N)>, `ID` map<int(N),int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[2]: ceil(sum(`c5`)) -> CAST(CAST(CAST((ceil(sum(`c5`))) AS TIME) AS STRUCT<a ARRAY<INT>, b MAP<INT, INT>>) AS JSON) \|\| marker: CAST((CAST((CAST((ceil(sum(`c5`))) AS TIME)) AS struct<`a` array<int(11)>, `b` map<int(11),int(11)>>)) AS JSON) \| M9-session: sql_ ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | array_agg(`ID`.`ID` ORDER BY `ID`.`ID` ASC `ID`.`ID` ASC) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> (array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)).`no such field` \|\| marker: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC).`no such field` |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(CAST(CAST(`ID`.`ID`.`ID` AS FLOAT) AS VARCHAR(N)) AS VARBINARY)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c3` -> CAST(CAST(CAST((`c3`) AS FLOAT) AS STRING) AS VARBINARY) \|\| marker: CAST((CAST((CAST(`c3` AS FLOAT)) AS VARCHAR(65533))) AS VARBINARY) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(array<tinyint(N)>, varchar, varchar, largeint(N)). | FunctionCallExpr[0]: `c2` -> array_agg(DISTINCT 2) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer#getArithmeticFunction | cast type array<varchar> with type smallint(N) is invalid. | ArithmeticExpr[1]: 1 -> split('a,b,c', ',') |

2963 further rare signatures not shown.


## Non-defect signature histogram (6 signatures, 6 instances)

| count | outcome | signature |
|---:|---|---|
| 1 | FIXPOINT_MISMATCH | fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:- N)) / N) < N AS `ID` FROM (SELECT covar_samp(`ID`.`ID`.`ID`, => - N)) / N) < N AS `ID` FROM (SELECT covar_samp(`ID`.`ID`.`ID`, |
| 1 | FIXPOINT_MISMATCH | fixpoint:'S')) / N) < N AS `ID` FROM (SELECT => 'S')) / N) < N AS `ID` FROM (SELECT |
| 1 | FIXPOINT_MISMATCH | fixpoint:((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID => ((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT sum(coalesce(N, N)) AS `ID` FROM => SELECT sum(coalesce(N, N)) AS `ID` FROM (SELECT `ID`.`ID`.`ID`, |
| 1 | FIXPOINT_MISMATCH | fixpoint:uzz_mut_115`ID`test_array_contains`ID`array_decimal128`ID`DB`ID`test_a => uzz_mut_115`ID`test_array_contains`ID`array_decimal128`ID`DB`ID`test_a |

## Non-defect outcomes (details for the 20 largest)

### FIXPOINT_MISMATCH — fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) (x1)
- mutant: SELECT ((abs(`result`.`co` - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532) < 0.00001`, `result`.`total` = (abs(`result`.`co` - 80)) AS `total = (abs(result.co - 80))` FROM (SELECT corr(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`ag ...
- detail: s1 ...0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...  |  s2 ...0.9988445981121532) < 1.0E-5 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...

### FIXPOINT_MISMATCH — fixpoint:- N)) / N) < N AS `ID` FROM (SELECT covar_samp(`ID`.`ID`.`ID`, => - N)) / N) < N AS `ID` FROM (SELECT covar_samp(`ID`.`ID`.`ID`, (x1)
- mutant: SELECT ((abs(`result`.`co` - 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001` FROM (SELECT covar_samp(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) AS `co` FROM `srfuzz_mut_61`.`aggtest` GROUP BY `srfuzz_mut_61`.`aggtest`.`no` LIMIT 1, 1) `result`
- detail: s1 ...- 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001` FROM (SELECT covar_samp(`srfuzz_mut_61`.`aggtest`.`k`,...  |  s2 ...- 120)) / 120) < 1.0E-5 AS `((abs(co - 120)) / 120) < 0.00001` FROM (SELECT covar_samp(`srfuzz_mut_61`.`aggtest`.`k`,...

### FIXPOINT_MISMATCH — fixpoint:'S')) / N) < N AS `ID` FROM (SELECT => 'S')) / N) < N AS `ID` FROM (SELECT (x1)
- mutant: SELECT ((abs(`result`.`co` - '1970-01-01')) / 120) < 0.00001 AS `((abs(co - '1970-01-01')) / 120) < 0.00001` FROM (SELECT covar_samp(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) OVER (PARTITION BY `srfuzz_mut_61`.`aggtest`.`no` ) AS `co` FROM `srfuzz_mut_61`.`aggtest` ORDER BY `co`  ...
- detail: s1 ...'1970-01-01')) / 120) < 0.00001 AS `((abs(co - '1970-01-01')) / 120) < 0.00001` FROM (SELECT...  |  s2 ...'1970-01-01')) / 120) < 1.0E-5 AS `((abs(co - '1970-01-01')) / 120) < 0.00001` FROM (SELECT...

### FIXPOINT_MISMATCH — fixpoint:((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID => ((abs(`ID`.`ID` - N)) / N) < N AS `ID` FROM (SELECT covar_pop(`ID`.`ID (x1)
- mutant: SELECT ((abs(`result`.`co` - 80)) / 80) < 0.00001 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`aggtest`.`k`, `aggtest`.`v`) AS `co` FROM (SELECT `srfuzz_mut_61`.`aggtest`.`no`, `srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v` FROM `srfuzz_mut_61`.`aggtest` UNION ALL SELEC ...
- detail: s1 ...((abs(`result`.`co` - 80)) / 80) < 0.00001 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`aggtest`.`k`,...  |  s2 ...((abs(`result`.`co` - 80)) / 80) < 1.0E-5 AS `((abs(co - 80)) / 80) < 0.00001` FROM (SELECT covar_pop(`aggtest`.`k`,...

### FIXPOINT_MISMATCH — fixpoint:SELECT sum(coalesce(N, N)) AS `ID` FROM => SELECT sum(coalesce(N, N)) AS `ID` FROM (SELECT `ID`.`ID`.`ID`, (x1)
- mutant: SELECT sum(coalesce(123456789012345678901234567890.123456789, 0)) AS `fingerprint` FROM (SELECT `srfuzz_mut_114`.`t1`.`v1`, `srfuzz_mut_114`.`t1`.`v2`, `srfuzz_mut_114`.`t1`.`v3`, array_agg(DISTINCT `srfuzz_mut_114`.`t1`.`v5`) OVER (PARTITION BY `srfuzz_mut_114`.`t1`.`v1` ) AS `arr_distinct` FROM `s ...
- detail: s1 ...SELECT sum(coalesce(123456789012345678901234567890.123456789, 0)) AS `fingerprint` FROM...  |  s2 ...SELECT sum(coalesce(1.2345678901234568E29, 0)) AS `fingerprint` FROM (SELECT `srfuzz_mut_114`.`t1`.`v1`,...

### FIXPOINT_MISMATCH — fixpoint:uzz_mut_115`ID`test_array_contains`ID`array_decimal128`ID`DB`ID`test_a => uzz_mut_115`ID`test_array_contains`ID`array_decimal128`ID`DB`ID`test_a (x1)
- mutant: SELECT `srfuzz_mut_115`.`test_array_contains`.`id` FROM `srfuzz_mut_115`.`test_array_contains` WHERE array_contains(`srfuzz_mut_115`.`test_array_contains`.`array_decimal128`, 56789012.34) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id` ASC
- detail: s1 ...uzz_mut_115`.`test_array_contains`.`array_decimal128`, 56789012.34) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...  |  s2 ...uzz_mut_115`.`test_array_contains`.`array_decimal128`, 5.678901234E7) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...
