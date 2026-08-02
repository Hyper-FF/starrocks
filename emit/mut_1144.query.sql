SELECT 'cluster still alive' AS `status`;
SELECT CAST((CAST('cluster still alive' AS VARCHAR)) AS VARBINARY) AS `status`;
SELECT CAST((CAST('cluster still alive' AS JSON)) AS INT) AS `status`;
SELECT 'cluster still alive' AS `status` LIMIT 0;
SELECT 'cluster still alive' AS `status` WHERE 'cluster still alive';
SELECT 'cluster still alive' AS `status` GROUP BY 'cluster still alive';
SELECT json_query(CAST('cluster still alive' AS JSON), '$.a') AS `status`;
SELECT 'cluster still alive' AS `status` GROUP BY 'cluster still alive';