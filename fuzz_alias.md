# AST Mutation Fuzz Report (P1)

seeds: 4330, mutants: 73481, dropped as grammar-unreachable: 10100 (12.1%)

seeds skipped as stale (no longer analyze where they are mutated): 1

## Dropped mutants (401 distinct reasons)

| count | reason | sample |
|---:|---|---|
| 9218 | deparse-threw:NullPointerException@com.starrocks.sql.analyzer.AnalyzerUtils#replaceNullType2Boolean | `<unrenderable>` |
| 156 | reparse-failed:ParsingException: Getting syntax error at line 2, column 34. Detail message: Unexpected input '{', the most similar input is {')'}. | `SELECT sum_map(`m`)[1], sum_map(`m`)[2], json_query(CAST((sum_map(`m`)[3]) AS JSON), '$.a') FROM (SELECT PseudoType.AnyMapType{1:10,2:20} AS `m` UNION ALL SELECT NULL AS `m` UNION ALL SELECT PseudoType.AnyMapType{1:5,3:30} AS `m`) `t`` |
| 59 | reparse-failed:ParsingException: Getting syntax error at line 1, column 28. Detail message: Unexpected input '{', the most similar input is {<EOF>, ';'}. | `SELECT PseudoType.AnyMapType{NULL:MAP<BOOLEAN,TINYINT>{NULL:1}}` |
| 56 | reparse-failed:ParsingException: Getting syntax error at line 1, column 62. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT round(covar_samp(`val1`, `val2`), PseudoType.AnyMapType{}) FROM `t1_nonnull` WHERE `k` > 100` |
| 45 | reparse-failed:ParsingException: Getting syntax error at line 3, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM `test_array_contains_complex_type` WHERE array_contains(`array_map`, PseudoType.AnyMapType{'a':1,'b':2147483647}) ORDER BY `id` ASC` |
| 22 | reparse-failed:ParsingException: Getting syntax error at line 1, column 47. Detail message: Unexpected input '{', the most similar input is {'ORDER', ')'}. | `SELECT array_agg(DISTINCT PseudoType.AnyMapType{}) FROM `ss` GROUP BY `id` ORDER BY 1 ASC` |
| 16 | reparse-failed:ParsingException: Getting syntax error at line 1, column 38. Detail message: No viable statement for input 'array_agg(PseudoType.AnyMapType{'. | `SELECT array_agg(PseudoType.AnyMapType{} ORDER BY `score` ASC) FROM `ss` GROUP BY `id` ORDER BY 1 ASC` |
| 11 | reparse-failed:ParsingException: Getting syntax error at line 5, column 55. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `id` FROM (SELECT * FROM `test_array_contains_complex_type` UNION SELECT * FROM `test_array_contains_complex_type`) `test_array_contains_complex_type` WHERE array_contains(`array_map`, PseudoType.AnyMapType{'a':1,'b':2}) ORDER BY `id` ASC` |
| 8 | reparse-failed:ParsingException: Getting syntax error at line 1, column 21. Detail message: Unexpected input '(', the most similar input is {<EOF>, ';'}. | `SELECT array_generate(9.`col`)` |
| 8 | reparse-failed:ParsingException: Getting syntax error at line 3, column 43. Detail message: No viable statement for input '(last_day(`dt`, PseudoType.AnyMapType{'. | `SELECT count(*) FROM `t1` WHERE (last_day(`dt`, PseudoType.AnyMapType{})) BETWEEN '2025-01-01' AND '2025-12-28'` |
| 6 | reparse-failed:ParsingException: Getting syntax error at line 1, column 54. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT approx_top_k(`c_tinyint`, PseudoType.AnyMapType{}) FROM `t_without_null`` |
| 6 | reparse-failed:ParsingException: Getting syntax error at line 1, column 19. Detail message: Unexpected input '(', the most similar input is {<EOF>, ';'}. | `SELECT approx_top_k(1.`c_partition`)` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 3, column 35. Detail message: No viable statement for input '(PseudoType.AnyMapType{'. | `SELECT DISTINCT `c2`, `c1` FROM `all_numbers_t0` WHERE `c2` = (PseudoType.AnyMapType{}) ORDER BY 1 ASC, 2 ASC  LIMIT 1` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 52. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT approx_top_k(`c_float`, PseudoType.AnyMapType{}) FROM `t_without_null`` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 65. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `c_partition`, approx_top_k(`c_int`, PseudoType.AnyMapType{}) FROM `t_partition` GROUP BY `c_partition` ORDER BY `c_partition` ASC` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 63. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT `c_id` % 4 AS `g`, min_n(`c_date`, PseudoType.AnyMapType{}) FROM `t_without_null` GROUP BY `g` ORDER BY `g` ASC` |
| 5 | reparse-failed:ParsingException: Getting syntax error at line 1, column 71. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT DISTINCT `id`, array_position(`array_map`, PseudoType.AnyMapType{'a':1,'b':2}) FROM `test_array_contains_complex_type` ORDER BY `id` ASC` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 3, column 19. Detail message: Unexpected input '[', the most similar input is {<EOF>, ';'}. | `SELECT round(covar_samp(`val1`, `val2`), 3) FROM `t1` WHERE `k` IN (5, 6)['k']` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 45. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT dict_merge(`c1`, PseudoType.AnyMapType{}) FROM `t2` GROUP BY `c1` ORDER BY `c1` ASC  LIMIT 10` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 28. Detail message: Unexpected input '`a`', the most similar input is {',', ')'}. | `SELECT min_n(`c_tinyint`, 1.`a`) FROM `t_without_null`` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 50. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT percentile_cont(`db`, PseudoType.AnyMapType{}) FROM `test_pc`` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 2, column 43. Detail message: Unexpected input '{', the most similar input is {')'}. | `SELECT sum_map(`m`) FROM (SELECT DISTINCT PseudoType.AnyMapType{} AS `m` UNION ALL SELECT PseudoType.AnyMapType{} AS `m` UNION ALL SELECT PseudoType.AnyMapType{} AS `m`) `t`` |
| 4 | reparse-failed:ParsingException: Getting syntax error at line 1, column 38. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT to_binary(PseudoType.AnyMapType{}, 'encode64')` |
| 3 | reparse-failed:ParsingException: Getting syntax error at line 1, column 53. Detail message: Unexpected input '{', the most similar input is {',', ')'}. | `SELECT approx_top_k(`c_bigint`, PseudoType.AnyMapType{}, 10) FROM `t_with_null`` |
| 3 | reparse-failed:ParsingException: Getting syntax error at line 3, column 16. Detail message: No viable statement for input '(2.`a`'. | `SELECT max(`id`), array_agg(DISTINCT `name`), count(DISTINCT `id`), array_agg(`name`) FROM `ss` WHERE `id` < (2.`a`) ORDER BY 1 ASC` |

| Outcome | Count |
|---|---:|
| OK | 50741 |
| ANALYZE_REJECTED | 27048 |
| ANALYZE_INTERNAL_ERROR | 15 |
| DEPARSE_THROW | 0 |
| REPARSE_FAIL | 0 |
| REANALYZE_FAIL | 1 |
| FIXPOINT_MISMATCH | 6 |

## Bug candidates (2 distinct signatures)

### ANALYZE_INTERNAL_ERROR — UnsupportedException@com.starrocks.sql.common.UnsupportedException#unsupportedException (x15)

- detail: UnsupportedException: Table function cannot appear on the left side of a join. Place it on the right side (optionally with LATERAL) or wrap it with TABLE(...).
- seed:     with w_empty as (select * from t1 where c1 = 'not-exist'), w1 as (select k1, k2, count(1) as cnt from t1 group by k1, k2), w2 as (select k1, k2, c1, count(1) as cnt from t1 group by k1, k2, c1) select count(1) from t1 tt1 join w_empty tt2 join t1 tt3 join w1 tt4 join w2 tt5
- mutation: M6-nesting TABLE_FUNCTION at JoinRelation.left: `t1` AS `tt1` INNER JOIN `w_empty` AS `tt2` INNER JOIN `t1` AS `tt3` INNER JOIN `w1` AS `tt4` -> (SELECT * FROM `t1` AS `tt1` INNER JOIN `w_empty` AS `tt2` INNER JOIN `t1` AS `tt3` INNER JOIN `w1` AS `tt4`) `srfuzz_n1`, generate_series(1, 3) `srfuzz_n2 ...
- mutant:   WITH `w_empty` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, `srfuzz_mut_7`.`t1`.`c1` FROM `srfuzz_mut_7`.`t1` WHERE `srfuzz_mut_7`.`t1`.`c1` = 'not-exist') , `w1` AS (SELECT `srfuzz_mut_7`.`t1`.`k1`, `srfuzz_mut_7`.`t1`.`k2`, count(1) AS `cnt` FROM `srfuzz_mut_7`.`t1` GROUP BY `srf ...

### REANALYZE_FAIL — SemanticException@com.starrocks.sql.analyzer.AggregationAnalyzer#analyze (x1)

- detail: SemanticException: Getting analyzing error from line 1, column 75 to line 1, column 267. Detail message: 'array_map(x -> array_concat([x], CAST(if(1 > rand(), '[]', '') AS ARRAY<VARCHAR(65533)>)), `srfuzz_mut_120`.`arra...
- seed:     select cast(if (1 > rand(), "[]", "") as array<string>) l , array_map((x)-> (concat(x,l)), arr_str) from array_map_x
- mutation: M5-clause select#0: add GROUP BY CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>), array_map(x -> concat(`x`, `l`), `arr_str`)
- mutant:   SELECT CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>) AS `l`, array_map(x -> array_concat(ARRAY<VARCHAR(65533)>[`__LAMBDA_TABLE`.`__LAMBDA_TABLE`.`x`], CAST((if(1 > (rand()), '[]', '')) AS ARRAY<VARCHAR(65533)>)), `srfuzz_mut_120`.`array_map_x`.`arr_str`) AS `array_map(x -> concat(x, CA ...

## Analyzer rejections (3664 signatures, 27048 instances)

### By operator

| operator | instances | share | signatures | most common reason |
|---|---:|---:|---:|---|
| M1-M4-expr | 13159 | 48.7% | 2016 | Unsupported nest aggregation function inside aggregation. |
| M7-typestress | 8902 | 32.9% | 1444 | Column 'S' cannot be resolved. |
| M5-clause | 3224 | 11.9% | 174 | Column 'S' cannot be resolved. |
| M6-nesting | 1763 | 6.5% | 30 | Column 'S' cannot be resolved. |

### Highest volume (40 of 3664 signatures)

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 2037 | M1-M4-expr | SemanticException@AggregationAnalyzer$VerifyExpressionVisitor#visitFunctionCall | Unsupported nest aggregation function inside aggregation. | FunctionCallExpr[0]: 1 -> count(1) |
| 1631 | M1-M4-expr | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | BinaryPredicate[0]: `c1` -> `tt1`.`k1` |
| 1078 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M6-nesting ASOF_JOIN at SelectRelation.from: `information_schema`.`column_stats_usage` -> (SELECT * FROM `information_schema`.`column_stats_usage`) `column_stats_usage` ASOF LEFT JOIN (SELECT * FROM `information_schema`.`column_stats_usage`) `srfuzz_ ... \| M9-session: enable_groupby_use_output_alias ... |
| 979 | M5-clause | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M5-clause select#1: add WHERE `srfuzz_mut_7`.`tt1`.`k1` = `srfuzz_mut_7`.`tt2`.`k1` |
| 744 | M5-clause | SemanticException@SelectAnalyzer#analyze | cannot combine SELECT DISTINCT with aggregate functions without GROUP BY. | M5-clause select#0: add DISTINCT |
| 690 | M5-clause | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | M5-clause select#2: remove GROUP BY `k1`, `k2`, `c1` |
| 680 | M7-typestress | SemanticException@Scope#resolveField | Column 'S' cannot be resolved. | M7-typestress STRUCT_FIELD at SelectListItem[1]: `column_name` -> (`column_name`).b \|\| marker: `column_name`.`b` |
| 503 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BIGINT because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: count(1) -> (count(1))[1] \|\| marker: count(1)[1] |
| 498 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type TINYINT because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[1]: 2 -> (2)[2] \|\| marker: 2[2] |
| 478 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR because it is not an array or a map or a struct. | M7-typestress MAP_KEY at WhereClause: `c1` = 'not-exist' -> (`c1` = 'not-exist')['k'] \|\| marker: `c1` = 'not-exist'['k'] |
| 419 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type INT because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[1]: `k2` -> (`k2`)[-1] \|\| marker: `k2`[-1] |
| 286 | M1-M4-expr | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | BinaryPredicate[0]: `c1` -> count(*) \| M9-session: enable_groupby_use_output_alias=true |
| 270 | M6-nesting | SemanticException@Scope#resolveField | Column 'S' is ambiguous. | M6-nesting ASOF_JOIN at JoinRelation.left: `w_empty` AS `tt1` INNER JOIN `t1` AS `tt2` INNER JOIN `t1` AS `tt3` -> (SELECT * FROM `w_empty` AS `tt1` INNER JOIN `t1` AS `tt2` INNER JOIN `t1` AS `tt3`) `srfuzz_n1` ASOF LEFT JOIN (SELECT * FROM `w_empty` AS `tt1` INNER JOIN `t1 ... |
| 270 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | array subscript must have type integer. | M7-typestress MAP_KEY at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> (array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC))['a.b'] \|\| marker: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)['a.b'] |
| 214 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_contains should be an array. | FunctionCallExpr[0]: <ArrayExpr> -> `srfuzz_mut_115`.`t`.`pk` |
| 203 | M1-M4-expr | SemanticException@AggregationAnalyzer#analyze | 'S' must be an aggregate expression or appear in GROUP BY clause. | FunctionCallExpr[0]: sum(murmur_hash3_32(`__col_1`)) -> ceil(murmur_hash3_32(`__col_1`)) |
| 200 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | GROUP BY clause cannot contain aggregations. | M5-clause select#0: add GROUP BY approx_top_k(`c_tinyint`) \| M9-session: enable_groupby_use_output_alias=true |
| 192 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedFunction | Table function cannot be used in expression. | M7-typestress JSON_PATH at SelectListItem[0]: count(*) -> json_each(CAST((count(*)) AS JSON)) \|\| marker: json_each(CAST((count(*)) AS JSON)) |
| 192 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_position should be an array. | FunctionCallExpr[0]: <ArrayExpr> -> 1 |
| 187 | M5-clause | SemanticException@AnalyzerUtils#verifyNoAggregateFunctions | WHERE clause cannot contain aggregations. | M5-clause select#0: add WHERE count(*) |
| 185 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type VARCHAR(N) because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at BinaryPredicate[0]: `table_database` -> (`table_database`)[1] \|\| marker: `table_database`[1] |
| 185 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | N-th input of arrays_overlap should be an array, rather than bigint(N). | FunctionCallExpr[0]: `s_1` -> `srfuzz_mut_129`.`array_test`.`pk` |
| 180 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | group_concat distinct should use constant separator. | FunctionCallExpr[2]: ',' -> sum(`t`.`c5`) |
| 173 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | N-th input of array_concat should be an array, rather than bigint(N). | FunctionCallExpr[1]: `s_1` -> `srfuzz_mut_118`.`array_test`.`pk` |
| 166 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | 'S' must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at CompoundPredicate[0]: (((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHAR(65533)))) OR ':' -> ((((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHAR(65533)))) OR ':').a \|\| marker: (((CAST(`c6` AS VARCHAR(65533))) OR ':') OR (CAST(`c7` AS VARCHA ... |
| 159 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DOUBLE because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: avg(`c3`) -> (avg(`c3`))[1] \|\| marker: avg(`c3`)[1] |
| 150 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_slice should be an array. | M7-typestress COLLECTION_FN at SelectListItem[0]: count(*) -> array_slice((count(*)), 1, 3) \|\| marker: array_slice(count(*), 1, 3) \| M9-session: sql_mode=288 |
| 145 | M1-M4-expr | SemanticException@SelectAnalyzer#analyzeOrderBy | ORDER BY position N is not in select list. | InPredicate[0]: `v1` -> `v1` |
| 134 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_remove should be an array. | FunctionCallExpr[0]: `s_1` -> `srfuzz_mut_118`.`array_test`.`pk` \| M9-session: enable_groupby_use_output_alias=true |
| 126 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The only one input of array_min should be an array, rather than int(N). | FunctionCallExpr[0]: array_unique_agg(`col_char`) -> `srfuzz_mut_38`.`test_array_agg`.`id` |
| 124 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The first input of array_slice should be an array. | FunctionCallExpr[0]: `COLB` -> `srfuzz_mut_111`.`flatten_test`.`COLA` |
| 109 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The only one input of array_max should be an array, rather than int(N). | FunctionCallExpr[0]: array_unique_agg(`col_boolean`) -> `srfuzz_mut_38`.`test_array_agg`.`id` |
| 90 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATE because it is not an array or a map or a struct. | M7-typestress MAP_KEY at SelectListItem[0]: `c18` -> (`c18`)[1] \|\| marker: `c18`[1] |
| 84 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type DATETIME because it is not an array or a map or a struct. | M7-typestress ARRAY_SUBSCRIPT at SelectListItem[6]: `c7` -> (`c7`)[0] \|\| marker: `c7`[0] |
| 84 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | percentile_cont 's second parameter should be constant and its type should be numeric. | FunctionCallExpr[1]: 0.5 -> `srfuzz_mut_21`.`t0`.`v1` |
| 84 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCollectionElementExpr | cannot subscript type BOOLEAN because it is not an array or a map or a struct. | M7-typestress MAP_KEY at WhereClause: (`c_id` >= 71) AND (`c_id` <= 90) -> ((`c_id` >= 71) AND (`c_id` <= 90))['0'] \|\| marker: (`c_id` >= 71) AND (`c_id` <= 90)['0'] |
| 81 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | percentile_disc_lc 's second parameter should be constant and its type should be numeric. | FunctionCallExpr[1]: 0.5 -> repeat('x', 65536) |
| 77 | M6-nesting | SemanticException@SelectAnalyzer#analyzeOrderBy | ORDER BY position N is not in select list. | M6-nesting TABLE_FUNCTION at SelectRelation.from: `t_cte_0` -> (SELECT * FROM `t_cte_0`) `t_cte_0`, generate_series(1, 3) `srfuzz_n1`(`e`) |
| 75 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | mann_whitney_u_test's third parameter should be a string literal.. | FunctionCallExpr[2]: 'two-sided' -> mann_whitney_u_test(`numbers`.`x`, `idx`.`t`, 'two-sided') |
| 70 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#checkFunction | The only one input of array_distinct should be an array, rather than bigint(N). | FunctionCallExpr[0]: `s_1` -> `srfuzz_mut_118`.`array_test`.`pk` |

3624 further signatures (12816 instances) below this cut.

### Rare signatures, count <= 2 (2534 of them, 3109 instances)

A false rejection would be here rather than above: one the analyzer should not have
issued is by nature uncommon. Read the message against the mutation next to it and ask
whether the mutant really was invalid.

| count | operator | throw site | message shape | sample mutation |
|---:|---|---|---|---|
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(CAST(`ID`.`ID`.`ID` AS FLOAT) AS VARBINARY)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c1` -> CAST(CAST((`c1`) AS FLOAT) AS VARBINARY) \|\| marker: CAST((CAST(`c1` AS FLOAT)) AS VARBINARY) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: unnest(smallint(N)). | M7-typestress COLLECTION_FN at SelectListItem[0]: `c2` -> unnest((`c2`)) \|\| marker: unnest(`c2`) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to array<int(N)> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[1]: `c7` -> CAST(CAST(CAST((`c7`) AS DATE) AS LARGEINT) AS ARRAY<INT>) \|\| marker: CAST((CAST((CAST(`c7` AS DATE)) AS LARGEINT)) AS ARRAY<INT>) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<map<varchar(N),int(N)>> to json in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[6]: `c8` -> CAST(CAST(CAST((`c8`) AS ARRAY<MAP<VARCHAR(16), INT>>) AS ARRAY<MAP<VARCHAR(16), INT>>) AS JSON) \|\| marker: CAST((CAST((CAST(`c8` AS ARRAY<MAP<VARCHAR(16),INT>>)) AS ARRAY<MAP<VARCHAR(16),INT>>)) AS JSON) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(`ID`.`ID`.`ID` AS JSON)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c1` -> CAST((`c1`) AS JSON) \|\| marker: CAST(`c1` AS JSON) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from largeint(N) to map<varchar(N),struct<`ID` int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c5` -> CAST(CAST((`c5`) AS MAP<VARCHAR(16), STRUCT<a INT>>) AS LARGEINT) \|\| marker: CAST((CAST(`c5` AS MAP<VARCHAR(16),struct<`a` int(11)>>)) AS LARGEINT) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#lambda$analyzeSelect$8 | DISTINCT can only be applied to comparable types : VARBINARY. | M7-typestress CAST_CHAIN at SelectListItem[2]: `c2` -> CAST(CAST(CAST((`c2`) AS VARCHAR) AS BIGINT) AS VARBINARY) \|\| marker: CAST((CAST((CAST(`c2` AS VARCHAR)) AS BIGINT)) AS VARBINARY) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from TIME to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[3]: `c13` -> CAST(CAST(CAST((`c13`) AS VARCHAR) AS TIME) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) \|\| marker: CAST((CAST((CAST(`c13` AS VARCHAR)) AS TIME)) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(((CAST(CAST(`ID`.`ID` AS VARCHAR(N)) AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CA | M7-typestress JSON_PATH at FunctionCallExpr[1]: ',' -> json_length(CAST((',') AS JSON)) \|\| marker: json_length(CAST(',' AS JSON)) \| M9-session: sql_mode=288 |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(((CAST(`ID`.`ID` AS BOOLEAN)) OR (CAST('S' AS BOOLEAN))) OR (CAST(CAST(`ID`.`ID` AS VARC | FunctionCallExpr[1]: ',' -> -1 |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | count(DISTINCT json_query(CAST(`ID`.`ID`.`ID` AS JSON), 'S')) can't rewrite distinct to group by on (json). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c3` -> json_query(CAST((`c3`) AS JSON), '$.a') \|\| marker: json_query(CAST(`c3` AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | count(DISTINCT CAST(`ID`.`ID`.`ID` AS JSON)) can't rewrite distinct to group by on (json). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c0` -> CAST((`c0`) AS JSON) \|\| marker: CAST(`c0` AS JSON) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from map<varchar(N),struct<`ID` int(N)>> to array<struct<`ID` int(N), `ID` varchar(N)>> in sql `ID`a`ID`. | M7-typestress CAST_CHAIN at CompoundPredicate[1]: `l`.`c0` <=> `r`.`c0` -> CAST(CAST(CAST((`l`.`c0` <=> `r`.`c0`) AS MAP<VARCHAR(16), STRUCT<a INT>>) AS ARRAY<STRUCT<a INT, b VARCHAR(8)>>) AS ARRAY<INT>) \|\| marker: CAST((CAST((CAST((`l`.`c0` <=> `r`.`c0`) AS MAP<VARCHAR(16),struct<`a` int(11)>>)) AS ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from datetime to array<array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[0]: `c0` -> CAST(CAST(CAST((`c0`) AS DATETIME) AS ARRAY<ARRAY<INT>>) AS TIME) \|\| marker: CAST((CAST((CAST(`c0` AS DATETIME)) AS ARRAY<ARRAY<INT>>)) AS TIME) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(DISTINCT `ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: sum(DISTINCT `c0`) -> (sum(DISTINCT `c0`)).c \|\| marker: sum(DISTINCT `c0`).`c` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | to_bitmap(`ID`.`ID`.`ID` + `ID`.`ID`.`ID`) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at FunctionCallExpr[0]: to_bitmap(`c0` + `c3`) -> (to_bitmap(`c0` + `c3`)).col \|\| marker: to_bitmap(`c0` + `c3`).`col` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: sum(array<varchar(N)>). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c0` -> CAST(CAST(CAST((`c0`) AS INT) AS CHAR(1)) AS ARRAY<VARCHAR(1)>) \|\| marker: CAST((CAST((CAST(`c0` AS INT)) AS CHAR(1))) AS ARRAY<VARCHAR(1)>) |
| 1 | M1-M4-expr | SemanticException@AggregationAnalyzer#analyze | 'S'week'S' must be an aggregate expression or appear in GROUP BY clause. | FunctionCallExpr[1]: `c4` -> `srfuzz_mut_19`.`skew_agg`.`c3` |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(CAST(`ID`.`ID`.`ID` AS VARCHAR), -N). | FunctionCallExpr[1]: ',' -> -1.0E308 |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: date_trunc(varchar, array<bigint(N)>). | FunctionCallExpr[1]: `c4` -> array_agg(`srfuzz_mut_19`.`skew_agg`.`c1`) \| M9-session: sql_mode=288 |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: length(array<varchar>). | FunctionCallExpr[0]: group_concat(CAST(`c3` AS VARCHAR) SEPARATOR ',') -> split(CAST(`c3` AS VARCHAR), ',') |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(CAST(`ID`.`ID`.`ID` AS VARCHAR), date_trunc('S', `ID`.`ID`.`ID`)). | FunctionCallExpr[1]: ',' -> date_trunc('week', `srfuzz_mut_19`.`skew_agg`.`c2`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: char_length(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> char_length(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: month(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> month(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: sqrt(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> sqrt(`a1`) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: split(varchar, array<varchar>). | FunctionCallExpr[1]: ',' -> split('a,b,c', ',') |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: abs(array<varchar>). | FunctionCallExpr[0]: array_agg(`a1`) -> abs(`a1`) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | count(DISTINCT CAST(`ID`.`ID`.`ID` AS JSON)->'S', `ID`.`ID`.`ID`, `ID`.`ID`.`ID`) can't rewrite distinct to group by on (json,bigint(N),bigint(N)). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c1` -> CAST((`c1`) AS JSON) -> '$.a' \|\| marker: CAST(`c1` AS JSON)->'$.a' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | sum(`ID`.`ID`.`ID` + if(`ID`.`ID`.`ID` IS NULL, N, N)) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[1]: sum(`t1`.`c2` + (if(`t2`.`c3` IS NULL, 1, 0))) -> (sum(`t1`.`c2` + (if(`t2`.`c3` IS NULL, 1, 0)))).`__col_3` \|\| marker: sum(`t1`.`c2` + (if(`t2`.`c3` IS NULL, 1, 0))).`__col_3` |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<bigint(N)> to largeint(N) in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> CAST((array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)) AS LARGEINT) \|\| marker: CAST((array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)) AS LARGEINT) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), -N ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC). | FunctionCallExpr[2]: ',' -> -1 |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(json_query(CAST(`ID`.`ID` AS JSON), 'S')). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c5` -> json_query(CAST((`c5`) AS JSON), '$.a') \|\| marker: json_query(CAST(`c5` AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT `ID`.`ID`, CAST(upper(`ID`.`ID`) AS JSON)->'S', 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) can't rewrite distinct to group by on (bigint | M7-typestress JSON_PATH at FunctionCallExpr[1]: upper(`c4`) -> CAST((upper(`c4`)) AS JSON) -> '$.a' \|\| marker: CAST((upper(`c4`)) AS JSON)->'$.a' |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, upper(`ID`.`ID`.`ID`), CAST('S' AS DATE) ORDER BY abs(`ID`.`ID` | M7-typestress CAST_CHAIN at FunctionCallExpr[2]: ',' -> CAST((',') AS DATE) \|\| marker: CAST(',' AS DATE) |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | array_agg(DISTINCT json_query(CAST(`ID`.`ID`.`ID` AS JSON), 'S')) can't rewrite distinct to group by on (json). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c2` -> json_query(CAST((`c2`) AS JSON), '$.a') \|\| marker: json_query(CAST(`c2` AS JSON), '$.a') \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M7-typestress | SemanticException@SelectAnalyzer#analyzeAggregations | group_concat(DISTINCT CAST(`ID`.`ID`.`ID` AS JSON)->'S', 'S') can't rewrite distinct to group by on (json,varchar). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `c2` -> CAST((`c2`) AS JSON) -> 'k' \|\| marker: CAST(`c2` AS JSON)->'k' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<bigint(N)> to array<array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at SelectListItem[1]: array_agg(DISTINCT `c2`) -> CAST(CAST(CAST((array_agg(DISTINCT `c2`)) AS ARRAY<ARRAY<INT>>) AS BOOLEAN) AS VARCHAR(1)) \|\| marker: CAST((CAST((CAST((array_agg(DISTINCT `c2`)) AS ARRAY<ARRAY<INT>>)) AS BOOLEAN)) AS VARCHAR(1)) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), json_length(CAST('S' AS JSON)) ORDER BY abs(`ID`.` | M7-typestress JSON_PATH at FunctionCallExpr[2]: ',' -> json_length(CAST((',') AS JSON)) \|\| marker: json_length(CAST(',' AS JSON)) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | array_agg(`ID`.`ID` ORDER BY `ID`.`ID` ASC `ID`.`ID` ASC) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[1]: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC) -> (array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC)).col \|\| marker: array_agg(`c3` ORDER BY `c3` ASC, `c4` ASC).`col` |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer#getArithmeticFunction | cast type array<tinyint(N)> with type bigint(N) is invalid. | ArithmeticExpr[0]: `c2` -> array_agg(DISTINCT 2) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | sum requires a numeric parameter: sum(CAST(CAST(`ID`.`ID`.`ID` AS DATE) AS DATETIME)). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c5` -> CAST(CAST((`c5`) AS DATE) AS DATETIME) \|\| marker: CAST((CAST(`c5` AS DATE)) AS DATETIME) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, CAST('S' AS JSON)->'S'). | M7-typestress JSON_PATH at FunctionCallExpr[1]: ',' -> CAST((',') AS JSON) -> 'k' \|\| marker: CAST(',' AS JSON)->'k' |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from char(N) to map<varchar(N),map<int(N),array<json>>> in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c3` -> CAST(CAST(CAST((`c3`) AS CHAR(1)) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) AS DECIMAL(38, 37)) \|\| marker: CAST((CAST((CAST(`c3` AS CHAR(1))) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>)) AS DECIMAL128(38,37)) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, CAST(CAST('S' AS BIGINT) AS TIME)). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: ',' -> CAST(CAST((',') AS BIGINT) AS TIME) \|\| marker: CAST((CAST(',' AS BIGINT)) AS TIME) |
| 1 | M1-M4-expr | SemanticException@ExpressionAnalyzer#getArithmeticFunction | cast type json with type smallint(N) is invalid. | ArithmeticExpr[0]: 1 -> CAST(NULL AS JSON) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | group_concat(DISTINCT `ID`.`ID`.`ID`, upper(`ID`.`ID`.`ID`), 'S' ORDER BY abs(`ID`.`ID`.`ID` + `ID`.`ID`.`ID`) ASC) must be a struct type, check if you are usin | M7-typestress STRUCT_FIELD at SelectListItem[0]: group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2` + `c3`) ASC SEPARATOR ',') -> (group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2` + `c3`) ASC SEPARATOR ',')).`a1` \|\| marker: group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2` + `c3 ... |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitSubfieldExpr | group_concat(DISTINCT `ID`.`ID`, upper(`ID`.`ID`), 'S' ORDER BY abs(`ID`.`ID` + `ID`.`ID`) ASC) must be a struct type, check if you are using `ID`. | M7-typestress STRUCT_FIELD at SelectListItem[0]: group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2` + `c3`) ASC SEPARATOR ',') -> (group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2` + `c3`) ASC SEPARATOR ',')).`__col_3` \|\| marker: group_concat(DISTINCT `c2`,upper(`c4`) ORDER BY abs(`c2`  ... |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, CAST('S' AS DATE)). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: ',' -> CAST((',') AS DATE) \|\| marker: CAST(',' AS DATE) \| M9-session: sql_mode=288 |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from array<array<int(N)>> to varchar in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c5` -> CAST(CAST(CAST((`c5`) AS ARRAY<JSON>) AS ARRAY<ARRAY<INT>>) AS VARCHAR) \|\| marker: CAST((CAST((CAST(`c5` AS ARRAY<JSON>)) AS ARRAY<ARRAY<INT>>)) AS VARCHAR) |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitCastExpr | Invalid type cast from varbinary to array<array<int(N)>> in sql `ID`. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c3` -> CAST(CAST((`c3`) AS VARBINARY) AS ARRAY<ARRAY<INT>>) \|\| marker: CAST((CAST(`c3` AS VARBINARY)) AS ARRAY<ARRAY<INT>>) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(DISTINCT `ID`.`ID`.`ID`, FALSE). | FunctionCallExpr[1]: ',' -> FALSE |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(array<bigint(N)>, varchar, varchar, largeint(N)). | FunctionCallExpr[0]: `c2` -> array_agg(`t`.`c3` ORDER BY `t`.`c3` ASC, `t`.`c4` ASC) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bigint(N), array<bigint(N)>, varchar, largeint(N)). | FunctionCallExpr[1]: upper(`c4`) -> array_agg(DISTINCT `srfuzz_mut_19`.`skew_agg`.`c3` ORDER BY `srfuzz_mut_19`.`skew_agg`.`c3` ASC, `srfuzz_mut_19`.`skew_agg`.`c4` ASC) \| M9-session: enable_groupby_use_output_alias=true |
| 1 | M7-typestress | SemanticException@ExpressionAnalyzer$Visitor#visitFunctionCall | No matching function with signature: abs(array<varchar(N)>). | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `c2` + `c3` -> CAST(CAST((`c2` + `c3`) AS JSON) AS ARRAY<VARCHAR(1)>) \|\| marker: CAST((CAST((`c2` + `c3`) AS JSON)) AS ARRAY<VARCHAR(1)>) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedFunction | No matching function with signature: group_concat(bigint(N), map<varchar(N),map<int(N),array<json>>>). | M7-typestress CAST_CHAIN at FunctionCallExpr[1]: ',' -> CAST(CAST((',') AS JSON) AS MAP<VARCHAR(16), MAP<INT, ARRAY<JSON>>>) \|\| marker: CAST((CAST(',' AS JSON)) AS MAP<VARCHAR(16),MAP<INT,ARRAY<JSON>>>) |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | avg requires a numeric parameter: avg(json_query(CAST(`ID`.`ID`.`ID` AS JSON), 'S')). | M7-typestress JSON_PATH at FunctionCallExpr[0]: `double_value` -> json_query(CAST((`double_value`) AS JSON), '$.a') \|\| marker: json_query(CAST(`double_value` AS JSON), '$.a') |
| 1 | M7-typestress | SemanticException@FunctionAnalyzer#getAnalyzedBuiltInFunction | Time Type can not used in max function. | M7-typestress CAST_CHAIN at FunctionCallExpr[0]: `decimal_value` -> CAST(CAST(CAST((`decimal_value`) AS DECIMAL(27, 9)) AS DECIMAL(1, 0)) AS TIME) \|\| marker: CAST((CAST((CAST(`decimal_value` AS DECIMAL128(27,9))) AS DECIMAL64(1,0))) AS TIME) |
| 1 | M1-M4-expr | SemanticException@FunctionAnalyzer#analyzeBuiltinAggFunction | group_concat requires separator to be of getType() STRING: group_concat(`ID`.`ID`.`ID`, count(`ID`.`ID`.`ID`) ORDER BY `ID`.`ID`.`ID` ASC). | FunctionCallExpr[1]: ',' -> count(`srfuzz_mut_20`.`test_agg_group_single_unique_key`.`varchar_value`) |
| 1 | M1-M4-expr | SemanticException@DecimalV3FunctionAnalyzer#getDecimalV3Function | No matching function with signature: truncate(boolean, bigint(N), bigint(N)).. | FunctionCallExpr[0]: if(`xx` < 1, `v2`, `v1` + 1) -> truncate(`xx` < 1, `v2`, `v1` + 1) |
| 1 | M1-M4-expr | SemanticException@DecimalV3FunctionAnalyzer#getDecimalV3Function | No matching function with signature: round(boolean, bigint(N), bigint(N)).. | FunctionCallExpr[0]: if(`xx` < 1, `v2`, `v1` + 1) -> round(`xx` < 1, `v2`, `v1` + 1) \| M9-session: sql_mode=288 |

2474 further rare signatures not shown.


## Non-defect signature histogram (5 signatures, 6 instances)

| count | outcome | signature |
|---:|---|---|
| 2 | FIXPOINT_MISMATCH | fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:SELECT ((abs(N - N)) / N) < N AS => SELECT ((abs(N - N)) / N) < N AS `((abs(N |
| 1 | FIXPOINT_MISMATCH | fixpoint:N) < N AS `((ceil(co - N)) / N) => N) < N AS `((ceil(co - N)) / N) |
| 1 | FIXPOINT_MISMATCH | fixpoint:- N)) / N) < N AS `ID`, `ID`.`ID` => - N)) / N) < N AS `ID`, `ID`.`ID` = |
| 1 | FIXPOINT_MISMATCH | fixpoint:fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a => fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a |

## Non-defect outcomes (details for the 20 largest)

### FIXPOINT_MISMATCH — fixpoint:N) < N AS `((abs(co - N)) / N) => N) < N AS `((abs(co - N)) / N) (x2)
- mutant: SELECT ((abs(`result`.`co` - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532) < 0.00001` FROM (SELECT corr(`aggtest`.`k`, `aggtest`.`v`) AS `co` FROM (SELECT `srfuzz_mut_61`.`aggtest`.`no`, `srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.` ...
- detail: s1 ...0.9988445981121532) < 0.00001 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...  |  s2 ...0.9988445981121532) < 1.0E-5 AS `((abs(co - 0.9988445981121532)) / 0.9988445981121532)...

### FIXPOINT_MISMATCH — fixpoint:SELECT ((abs(N - N)) / N) < N AS => SELECT ((abs(N - N)) / N) < N AS `((abs(N (x1)
- mutant: SELECT ((abs(0.00001 - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS `((abs(0.00001 - 0.9988445981121532)) / 0.9988445981121532) < 0.00001` FROM (SELECT corr(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) AS `co` FROM `srfuzz_mut_61`.`aggtest` GROUP BY `srfuzz_mut_61`.`aggte ...
- detail: s1 ...SELECT ((abs(0.00001 - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS...  |  s2 ...SELECT ((abs(1.0E-5 - 0.9988445981121532)) / 0.9988445981121532) < 1.0E-5 AS `((abs(0.00001...

### FIXPOINT_MISMATCH — fixpoint:N) < N AS `((ceil(co - N)) / N) => N) < N AS `((ceil(co - N)) / N) (x1)
- mutant: SELECT ((ceil(`result`.`co` - 0.9988445981121532)) / 0.9988445981121532) < 0.00001 AS `((ceil(co - 0.9988445981121532)) / 0.9988445981121532) < 0.00001`, `result`.`total` = 4 AS `total = 4` FROM (SELECT corr(`srfuzz_mut_61`.`aggtest`.`k`, `srfuzz_mut_61`.`aggtest`.`v`) AS `co`, count(DISTINCT `srfuz ...
- detail: s1 ...0.9988445981121532) < 0.00001 AS `((ceil(co - 0.9988445981121532)) / 0.9988445981121532)...  |  s2 ...0.9988445981121532) < 1.0E-5 AS `((ceil(co - 0.9988445981121532)) / 0.9988445981121532)...

### FIXPOINT_MISMATCH — fixpoint:- N)) / N) < N AS `ID`, `ID`.`ID` => - N)) / N) < N AS `ID`, `ID`.`ID` = (x1)
- mutant: SELECT ((abs(`result`.`co` - 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total` = 4 AS `total = 4` FROM (SELECT covar_samp(`aggtest`.`k`, `aggtest`.`v`) AS `co`, count(DISTINCT `aggtest`.`k`) AS `total` FROM (SELECT `srfuzz_mut_61`.`aggtest`.`no`, `srfuzz_mut_61`.`aggtes ...
- detail: s1 ...- 120)) / 120) < 0.00001 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total`...  |  s2 ...- 120)) / 120) < 1.0E-5 AS `((abs(co - 120)) / 120) < 0.00001`, `result`.`total` =...

### FIXPOINT_MISMATCH — fixpoint:fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a => fuzz_mut_115`ID`test_array_contains`ID`array_decimal64`ID`DB`ID`test_a (x1)
- mutant: SELECT `srfuzz_mut_115`.`test_array_contains`.`id` FROM `srfuzz_mut_115`.`test_array_contains` WHERE array_contains(`srfuzz_mut_115`.`test_array_contains`.`array_decimal64`, 12345678.90) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id` ASC
- detail: s1 ...fuzz_mut_115`.`test_array_contains`.`array_decimal64`, 12345678.90) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...  |  s2 ...fuzz_mut_115`.`test_array_contains`.`array_decimal64`, 1.23456789E7) ORDER BY `srfuzz_mut_115`.`test_array_contains`.`id`...
