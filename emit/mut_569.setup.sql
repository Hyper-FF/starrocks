CREATE TABLE if not exists variant_test_unified (
    id INT,
    test_scenario STRING,  
    test_case STRING,
    int_val INT,
    bigint_val BIGINT,
    float_val FLOAT,
    double_val DOUBLE,
    decimal_val DECIMAL(10, 2),
    string_val STRING,
    boolean_val BOOLEAN,
    variant_from_int VARIANT,
    variant_from_bigint VARIANT,
    variant_from_float VARIANT,
    variant_from_double VARIANT,
    variant_from_decimal VARIANT,
    variant_from_string VARIANT,
    variant_from_boolean VARIANT,
    struct_data STRUCT<
        user_id INT,
        profile STRUCT<
            name STRING,
            age INT,
            contact STRUCT<
                email STRING,
                phones ARRAY<STRING>,
                address STRUCT<
                    city STRING,
                    zip STRING,
                    coordinates STRUCT<lat DOUBLE, lng DOUBLE>
                >
            >
        >,
        preferences MAP<STRING, STRING>,
        tags ARRAY<STRING>
    >,
    array_data ARRAY<STRUCT<
        id INT,
        name STRING,
        `values` ARRAY<DOUBLE>,
        metadata MAP<STRING, STRING>
    >>,
    map_data MAP<STRING, STRUCT<
        count INT,
        percentage DOUBLE,
        active BOOLEAN,
        nested MAP<STRING, INT>
    >>,
    variant_struct_data VARIANT,
    variant_array_data VARIANT,
    variant_map_data VARIANT,
    variant_simple_struct VARIANT,
    variant_simple_array VARIANT,
    variant_simple_map VARIANT,
    variant_deep_nested VARIANT
) PROPERTIES (
    'format-version' = '3'  
);
drop table variant_test_unified force;