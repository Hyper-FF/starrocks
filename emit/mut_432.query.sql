SELECT count(*) AS `count(*)` FROM `srfuzz_mut_432`.`t0`;
SELECT __iceberg_transform_truncate(`srfuzz_mut_432`.`t0`.`i32`, 10) AS `__iceberg_transform_truncate(i32, 10)` FROM `srfuzz_mut_432`.`t0`;
SELECT __iceberg_transform_bucket(`srfuzz_mut_432`.`t0`.`i32`, 8) AS `__iceberg_transform_bucket(i32, 8)` FROM `srfuzz_mut_432`.`t0`;