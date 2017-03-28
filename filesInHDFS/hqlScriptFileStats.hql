
DROP TABLE IF EXISTS retail2.stats;
CREATE EXTERNAL TABLE IF NOT EXISTS retail2.stats (
state string
,department string
,order_status string
,order_total decimal(18,2)
,order_count bigint
)
PARTITIONED BY (`trans_date` string)
STORED AS PARQUET;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE retail2.stats PARTITION(trans_date)
SELECT
emp.us_state_code_abbr as state,
d.department_name,
od.order_status, 
round(sum(od.order_item_subtotal),2) as order_total,
count(od.order_item_subtotal) as order_count,
trans_date from retail2_stg.order_detail od
inner join retail2_stg.products p on (od.order_item_product_id = p.product_id) 
inner join retail2_stg.categories c on (p.product_category_id = c.category_id) 
inner join retail2_stg.departments d on (d.department_id = c.category_department_id),
retail2_stg.us_state_reg_emp_br emp
where (od.trans_date <= emp.comm_end_eff_date) AND (od.trans_date >= emp.comm_start_eff_date) 
group by emp.us_state_code_abbr, d.department_name, od.order_status,trans_date;
