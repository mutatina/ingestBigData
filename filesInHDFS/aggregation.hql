
DROP TABLE IF EXISTS retail2.temptable1;
DROP TABLE IF EXISTS retail2.temptable2;
DROP TABLE IF EXISTS retail2.temptable3;
DROP TABLE IF EXISTS retail2.final;

CREATE TABLE retail2.temptable1 AS SELECT DISTINCT employee_name,commission_rate,us_state_code_abbr,comm_start_eff_date,comm_end_eff_date FROM retail2_geo.employee, retail2_geo.us_state_reg_emp_br as usreb  WHERE  employee.employee_id = usreb.employee_id;

CREATE TABLE retail2.temptable2 AS select us_state_code_name, employee_name,commission_rate,comm_start_eff_date, comm_end_eff_date FROM retail2_geo.us_state_codes,retail2.temptable1 where us_state_codes.us_state_code_abbr = temptable1.us_state_code_abbr;

CREATE TABLE retail2.temptable3 as select order_id,order_status,trans_date,sum(order_item_subtotal) as sales_amount,count(order_id) as number_of_orders from retail2.order_detail group by order_id,order_status,trans_date;

CREATE TABLE retail2.final as SELECT  us_state_code_name , employee_name ,round(( commission_rate * sales_amount),2)as commission_earned ,order_status  , trans_date , round(sales_amount,2) as sales_amount ,order_id, number_of_orders  from retail2.temptable2,retail2.temptable3 as T3 where T3.trans_date between temptable2.comm_start_eff_date and temptable2.comm_end_eff_date limit 100,000;


DROP TABLE IF EXISTS retail2.commission;
CREATE EXTERNAL TABLE IF NOT EXISTS retail2.commission (
us_state_code_name string
,employee_name string
,commission_earned DECIMAL(26,2)
,order_status string
,sales_amount DECIMAL(18,2)
,order_id int
,number_of_orders bigint
)
PARTITIONED BY (`trans_date` string)
STORED AS PARQUET;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE retail2.commission PARTITION(trans_date)
SELECT * FROM retail2.final;








