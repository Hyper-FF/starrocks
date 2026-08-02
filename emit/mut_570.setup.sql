CREATE TABLE variant_boundary_arrays_test (
    test_id INT,
    test_label STRING,
    element_count INT,
    variant_col VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_boundary_nesting_test (
    test_id INT,
    test_label STRING,
    nesting_level INT,
    variant_col VARIANT
)
properties(
  'format-version' = '3'
);
CREATE TABLE variant_boundary_numeric_test (
    test_id INT,
    test_label STRING,
    numeric_type STRING,
    variant_col VARIANT,
    expected_value STRING
)
properties(
  'format-version' = '3'
);
drop table variant_boundary_arrays_test force;
drop table variant_boundary_nesting_test force;
drop table variant_boundary_numeric_test force;