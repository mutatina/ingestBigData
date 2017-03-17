

---Example:
--- INSERT INTO TABLE retail.order_details_test PARTITION(trans_date)
---  SELECT * FROM retail_stg_test.order_details_test2 ;

---Parameterized:
---Needs 6 parameters( finaldb name, stage database name, table name(same for both) partition column, min load date , max load date)
 


SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE ${target_parquet_db}.${target_table} PARTITION (${partition_column})
SELECT *
FROM ${target_stg_database}.${target_table};


