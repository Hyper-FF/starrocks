CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );
insert into aggtest values(1, 10, NULL);
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);
CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );
insert into aggtest values(1, 10, NULL);
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);
CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );
insert into aggtest values(1, 10, NULL);
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);