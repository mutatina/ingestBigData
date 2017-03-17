#! /usr/bin/env python

from impala.dbapi import connect
import mysql.connector,time,os,sys,subprocess



user=
password=
host='127.0.0.1'
src_database=
src_table=
target_database=
target_table=
isPartitioned=eval()
partition_column=
min_load_date=
max_load_date=
impalaHost='quickstart.cloudera'
impalaPort=21050
target_stg_database
target_parquet_db


localtime = time.asctime(time.localtime(time.time()))
log=''
log = log + '\nValidator ran at  = '+ str(localtime)

config = {'user': user, 'password': password, 'host': host,'src_database': src_database}  

cnx =mysql.connector.MySQLConnection(**config)

if isPartitioned == True:
	query = "SELECT COUNT(*) FROM"+ src_database +"."+src_table +" AS T WHERE T."+ partition_column+" BETWEEN "+ min_load_date +" AND "+ max_load_date +";"
else:
	query = "SELECT COUNT(*) FROM" + src_database +"."+src_table +";"


cursor = cnx.cursor()
cursor.execute(query)          
total_mysql_rows = cursor.fetchone()[0]   
#f.write('\nTotal mysql rows  = ' + str(total_mysql_rows) ) 
log = log +'\nTotal mysql rows  = ' + str(total_mysql_rows)

#Checking Impala Stage and Final:
conn = connect(host=impalaHost, port=impalaPort)
cur = conn.cursor()

if isPartitioned == True:
	query = "SELECT COUNT(*) FROM"+ target_stg_database +"."+target_table +" AS T WHERE T."+ partition_column+" BETWEEN "+ min_load_date +" AND "+ max_load_date +";"
else:
	query = "SELECT COUNT(*) FROM" + target_stg_database +"."+target_table +";"


cur.execute('select count(*) from retail_stg_test.order_details_test2')
result_array=cur.fetchall()
total_impala_rows =result_array[0][0]
#f.write('\nTotal impala rows in Stage Database = ' + str(total_impala_rows) ) 
log = log +'\nTotal impala rows in stage = ' + str(total_impala_rows) 


if isPartitioned == True:
	query = "SELECT COUNT(*) FROM"+ target_parquet_db +"."+target_table +" AS T WHERE T."+ partition_column+" BETWEEN "+ min_load_date +" AND "+ max_load_date +";"
else:
	query = "SELECT COUNT(*) FROM" + target_parquet_db +"."+target_table +";"



cur.execute('select count(*) from retail.order_details_test2')
result_array=cur.fetchall()
total_impala_rows =result_array[0][0]
#f.write('\nTotal impala rows in FInal Database = ' + str(total_impala_rows) ) 
log = log +'\nTotal impala rows in final = ' + str(total_impala_rows)

log= '"'+log + '"'
print log

#_command = "echo " + log + "| hadoop fs -appendToFile - /user/cloudera/oozie/logFolder/log.txt"

#subprocess.Popen(_command,shell=True)

