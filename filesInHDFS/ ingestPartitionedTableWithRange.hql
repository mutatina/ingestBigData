

---Example:
--- INSERT INTO TABLE retail.order_details_test PARTITION(trans_date)
---  SELECT * FROM retail_stg_test.order_details_test2 AS stg_tbl
---  where stg_tbl.trans_date BETWEEN '2017-01-27' AND '2017-01-27';

---Parameterized:
---Needs 6 parameters( finaldb name, stage database name, table name(same for both) partition column, min load date , max load date)
 


SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE ${target_parquet_db}.${target_table} PARTITION (${partition_column})
SELECT *
FROM ${target_stg_database}.${target_table} AS stg_tbl
WHERE stg_tbl.${partition_column} BETWEEN '${min_load_date}' AND '${max_load_date}';


