SELECT DISTINCT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` ORDER BY 1 ASC  LIMIT 10;
SELECT DISTINCT `string300`.`v1` FROM (SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` INTERSECT SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300`) `string300` ORDER BY 1 ASC  LIMIT 10;
SELECT DISTINCT `string300`.`v1` FROM (SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` EXCEPT SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300`) `string300` ORDER BY 1 ASC  LIMIT 10;
SELECT DISTINCT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` ORDER BY 1 ASC;
SELECT DISTINCT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` LIMIT 10;
SELECT DISTINCT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` GROUP BY `srfuzz_mut_449`.`string300`.`v1` ORDER BY 1 ASC  LIMIT 10;
SELECT DISTINCT CAST((CAST(`srfuzz_mut_449`.`string300`.`v1` AS INT)) AS DOUBLE) AS `CAST((CAST(v1 AS INT)) AS DOUBLE)` FROM `srfuzz_mut_449`.`string300` ORDER BY 1 ASC  LIMIT 10;
SELECT DISTINCT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` LIMIT 10;
SELECT DISTINCT `string300`.`v1` FROM (SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300` UNION ALL SELECT `srfuzz_mut_449`.`string300`.`v1` FROM `srfuzz_mut_449`.`string300`) `string300` ORDER BY 1 ASC  LIMIT 10;