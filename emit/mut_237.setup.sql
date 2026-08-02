CREATE TABLE `employees` (
  `employee_id` int(11) NULL COMMENT "",
  `name` varchar(100) NULL COMMENT "",
  `manager_id` int(11) NULL COMMENT "",
  `title` varchar(50) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`employee_id`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
INSERT INTO employees VALUES
(1, 'Alicia', NULL, 'CEO'),
(2, 'Bob', 1, 'CTO'),
(3, 'Carol', 1, 'CFO'),
(4, 'David', 2, 'VP of Engineering'),
(5, 'Eve', 2, 'VP of Research'),
(6, 'Frank', 3, 'VP of Finance'),
(7, 'Grace', 4, 'Engineering Manager'),
(8, 'Heidi', 4, 'Tech Lead'),
(9, 'Ivan', 5, 'Research Manager'),
(10, 'Judy', 7, 'Senior Engineer'),
(11, 'Kevin', 6, 'Finance Manager'),
(12, 'Laura', 6, 'Accountant'),
(13, 'Mike', 8, 'Senior Engineer'),
(14, 'Nancy', 8, 'Engineer'),
(15, 'Oscar', 7, 'Software Engineer'),
(16, 'Peter', 7, 'QA Engineer'),
(17, 'Quinn', 9, 'Research Scientist'),
(18, 'Rachel', 9, 'Data Scientist'),
(19, 'Steve', 11, 'Financial Analyst'),
(20, 'Tina', 13, 'Engineer'),
(21, 'Uma', 13, 'Junior Engineer'),
(22, 'Victor', 15, 'Junior Developer'),
(23, 'Wendy', 17, 'Research Associate'),
(24, 'Xavier', 18, 'ML Engineer'),
(25, 'Yolanda', 14, 'Junior Engineer');