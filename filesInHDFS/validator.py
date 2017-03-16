#! /usr/bin/env python
from impala.dbapi import connect
import mysql.connector
import time
import os
import sys
import subprocess
localtime = time.asctime( time.localtime(time.time()) )



log=''
log = log + '\nValidator ran at  = '+ str(localtime)


config = {'user': 'root', 'password': 'cloudera', 'host': '127.0.0.1','database': 'retail_db'}  
cnx =mysql.connector.MySQLConnection(**config)
query = "SELECT COUNT(*) from temp3"
cursor = cnx.cursor()
cursor.execute(query)          
total_mysql_rows = cursor.fetchone()[0]   
#f.write('\nTotal mysql rows  = ' + str(total_mysql_rows) ) 
log = log +'\nTotal mysql rows  = ' + str(total_mysql_rows)

conn = connect(host='quickstart.cloudera', port=21050)
cur = conn.cursor()
cur.execute('select count(*) from retail_stg_test.order_details_test2')
result_array=cur.fetchall()
total_impala_rows =result_array[0][0]
#f.write('\nTotal impala rows in retail_stg_test = ' + str(total_impala_rows) ) 
log = log +'\nTotal impala rows in retail_stg = ' + str(total_impala_rows) 

cur.execute('select count(*) from retail.order_details_test2')
result_array=cur.fetchall()
total_impala_rows =result_array[0][0]
#f.write('\nTotal impala rows in retail = ' + str(total_impala_rows) ) 
log = log +'\nTotal impala rows in retail = ' + str(total_impala_rows)

log= '"'+log + '"'
print log
_command = "echo " + log + "| hadoop fs -appendToFile - /user/cloudera/oozie/logFolder/log.txt"

subprocess.Popen(_command,shell=True)

