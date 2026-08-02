CREATE TABLE single_row_data (
    dim1 STRING,    
    dim2 STRING,    
    dim3 STRING,    
    dim4 STRING,    
    sales INT,      
    quantity INT,  
    cost INT       
) DUPLICATE KEY(dim1) 
DISTRIBUTED BY HASH(dim1) BUCKETS 1 
PROPERTIES('replication_num' = '1');
INSERT INTO single_row_data VALUES ('华东', '上海', '浦东店', '电器部', 1000, 50, 600);