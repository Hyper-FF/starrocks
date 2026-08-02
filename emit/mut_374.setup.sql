create table t_incorrect_slot_id (
    val BIGINT NOT NULL,
    auto_inc_id  BIGINT AUTO_INCREMENT
) ENGINE = olap
DUPLICATE KEY(val)
PROPERTIES (
"compression" = "LZ4",
"replication_num" = "1"
);
drop table t_incorrect_slot_id;