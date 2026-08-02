CREATE TABLE user_behavior (
    id BIGINT,
    user_profile STRUCT<
        user_id INT,
        user_name STRING,
        user_level INT,
        registration_date DATE,
        location STRUCT<
            country STRING,
            city STRING,
            coordinates STRUCT<
                latitude DOUBLE,
                longitude DOUBLE
            >
        >,
        preferences STRUCT<
            category STRING,
            sub_category STRING,
            score INT
        >
    >,
    event_timestamp DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
INSERT INTO user_behavior
SELECT
    generate_series AS id,
    row(
        CAST(generate_series % 1000000 AS INT),
        CONCAT('user_', CAST(generate_series % 1000000 AS STRING)),
        CAST(generate_series % 10 AS INT),
        DATE_ADD('2020-01-01', INTERVAL (generate_series % 1460) DAY),
        row(
            CASE generate_series % 10
                WHEN 0 THEN 'USA'
                WHEN 1 THEN 'China'
                WHEN 2 THEN 'UK'
                WHEN 3 THEN 'Japan'
                WHEN 4 THEN 'Germany'
                WHEN 5 THEN 'France'
                WHEN 6 THEN 'Canada'
                WHEN 7 THEN 'Australia'
                WHEN 8 THEN 'India'
                ELSE 'Brazil'
            END,
            CONCAT('City_', CAST(generate_series % 100 AS STRING)),
            row(
                CAST(20.0 + (generate_series % 60) AS DOUBLE),
                CAST(-180.0 + (generate_series % 360) AS DOUBLE)
            )
        ),
        row(
            CASE generate_series % 5
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Books'
                WHEN 2 THEN 'Clothing'
                WHEN 3 THEN 'Food'
                ELSE 'Sports'
            END,
            CONCAT('Sub_', CAST(generate_series % 20 AS STRING)),
            CAST(generate_series % 100 AS INT)
        )
    ) AS user_profile,
    date_add(CAST('2024-01-01 00:00:00' AS DATETIME), INTERVAL (generate_series % 86400) SECOND) AS event_timestamp
FROM TABLE(generate_series(1, 1000000));
CREATE TABLE transaction_data (
    id BIGINT,
    transaction_info STRUCT<
        card STRUCT<
            card_details STRUCT<
                card_bin STRING,
                card_type STRING,
                issuer STRING,
                card_program STRING
            >,
            card_tags ARRAY<STRUCT<value STRING>>,
            version INT
        >,
        transaction STRUCT<
            transaction_id STRING,
            transaction_details STRUCT<
                transaction_type STRING,
                merchant_data STRUCT<
                    merchant_id STRING,
                    merchant_name STRING,
                    merchant_country_code STRING
                >,
                posting_date STRING,
                amount STRUCT<
                    currency_code STRING
                >,
                billing_amount STRUCT<
                    currency_code STRING
                >,
                billing_amount_no_fee DECIMAL(18, 4),
                conversion_rate DECIMAL(18, 4),
                ird STRING
            >
        >
    >
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
INSERT INTO transaction_data VALUES
(1, row(
    row(
        row('411111', 'VISA', 'Chase', 'Rewards'),
        [row('Premium'), row('Verified')],
        1
    ),
    row(
        'TXN001',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'US'),
            '2024-01-15',
            row('USD'),
            row('USD'),
            100.5000,
            1.0000,
            'IRD001'
        )
    )
)),
(2, row(
    row(
        row('520000', 'MASTERCARD', 'Citi', 'CashBack'),
        [row('Standard')],
        2
    ),
    row(
        'TXN002',
        row(
            'REFUND',
            row('MERCH002', 'Walmart', 'US'),
            '2024-01-16',
            row('USD'),
            row('USD'),
            50.2500,
            1.0000,
            'IRD002'
        )
    )
)),
(3, row(
    row(
        row('370000', 'AMEX', 'American Express', 'Platinum'),
        [row('Premium'), row('Travel'), row('Insurance')],
        1
    ),
    row(
        'TXN003',
        row(
            'PURCHASE',
            row('MERCH003', 'Apple Store', 'US'),
            '2024-01-17',
            row('USD'),
            row('EUR'),
            1200.0000,
            0.9200,
            'IRD003'
        )
    )
)),
(4, row(
    row(
        row('411111', 'VISA', 'Bank of America', 'Travel'),
        [row('Gold')],
        1
    ),
    row(
        'TXN004',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'UK'),
            '2024-01-18',
            row('GBP'),
            row('USD'),
            85.7500,
            1.2700,
            'IRD004'
        )
    )
)),
(5, row(
    row(
        row('520000', 'MASTERCARD', 'HSBC', 'Standard'),
        [row('Basic')],
        2
    ),
    row(
        'TXN005',
        row(
            'WITHDRAWAL',
            row('MERCH004', 'ATM Network', 'CN'),
            '2024-01-19',
            row('CNY'),
            row('USD'),
            500.0000,
            0.1400,
            'IRD005'
        )
    )
)),
(6, row(
    row(
        row('370000', 'AMEX', 'American Express', 'Gold'),
        [row('Premium'), row('Concierge')],
        3
    ),
    row(
        'TXN006',
        row(
            'PURCHASE',
            row('MERCH005', 'Luxury Hotel', 'FR'),
            '2024-01-20',
            row('EUR'),
            row('USD'),
            2500.0000,
            1.0800,
            'IRD006'
        )
    )
)),
(7, row(
    row(
        row('411111', 'VISA', 'Chase', 'Rewards'),
        [row('Verified')],
        1
    ),
    row(
        'TXN007',
        row(
            'PURCHASE',
            row('MERCH001', 'Amazon', 'US'),
            '2024-01-21',
            row('USD'),
            row('USD'),
            45.9900,
            1.0000,
            'IRD007'
        )
    )
)),
(8, row(
    row(
        row('520000', 'MASTERCARD', 'Citi', 'CashBack'),
        [row('Standard'), row('Contactless')],
        2
    ),
    row(
        'TXN008',
        row(
            'PURCHASE',
            row('MERCH006', 'Grocery Store', 'CA'),
            '2024-01-22',
            row('CAD'),
            row('USD'),
            75.0000,
            0.7500,
            'IRD008'
        )
    )
));
DROP TABLE transaction_data FORCE;