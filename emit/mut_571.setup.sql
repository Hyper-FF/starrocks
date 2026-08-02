CREATE TABLE variant_fallback_paths_test (
    test_id INT,
    test_label STRING,
    variant_col VARIANT,
    test_path STRING,
    expected_result STRING
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_fallback_types_test (
    test_id INT,
    test_label STRING,
    variant_col VARIANT,
    variant_type STRING,
    test_path STRING,
    access_type STRING
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_fallback_cast_test (
    test_id INT,
    test_label STRING,
    variant_col VARIANT,
    source_type STRING,
    target_cast STRING,
    expected_behavior STRING
)
properties(
  'format-version' = '3'
);
drop table variant_fallback_paths_test force;
drop table variant_fallback_types_test force;
drop table variant_fallback_cast_test force;