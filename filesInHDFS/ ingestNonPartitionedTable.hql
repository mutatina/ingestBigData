

---Example:
  
---INSERT INTO TABLE retail.us_regions SELECT * FROM retail_stg_test.us_regions;

---Parameterized: 

---Needs 3 parameters: finaldb name, stage db name, table name(same for both) 




INSERT OVERWRITE TABLE ${target_parquet_db}.${target_table}  SELECT * FROM ${target_stg_database}.${target_table};



