#! /usr/bin/env python
"""
Name: validator.py
Descritption:This module counts  rows beteween impala and mysql per given range if given or coun t all the rows per given database-table combination.
Command Line arguments: user,password,host,src_database,src_table,target_table,isPartitioned,partition_column,impalaHost,impalaPort,
			target_stg_database,target_parquet_db,logFolder,min_load_date,max_load_date

"""
from impala.dbapi import connect
import mysql.connector,time,os,sys,subprocess,optparse
from optparse import OptionParser

def argumentParser():
	parser = OptionParser()
	parser.add_option("--user", dest="user")
	parser.add_option("--password", dest="password")
	parser.add_option("--host", dest="host")
	parser.add_option("--src_database", dest="src_database")
	parser.add_option("--src_table", dest="src_table")
	parser.add_option("--target_table", dest="target_table")
	parser.add_option("--isPartitioned", dest="isPartitioned")
	parser.add_option("--partition_column", dest="partition_column")		
	parser.add_option("--impalaHost", dest="impalaHost")
	parser.add_option("--impalaPort", dest="impalaPort")
	parser.add_option("--target_stg_database", dest="target_stg_database")
	parser.add_option("--target_parquet_db", dest="target_parquet_db")
	parser.add_option("--logFolder", dest="logFolder")
	parser.add_option("--min_load_date", dest="min_load_date")
	parser.add_option("--max_load_date", dest="max_load_date")
	
	(options, args) = parser.parse_args()	
	return options.user,options.password,options.host,options.src_database,options.src_table,options.target_table,\ 
	options.isPartitioned,options.partition_column,options.impalaHost,options.impalaPort,options.target_stg_database,options.target_parquet_db,\
        options.logFolder,options.min_load_date,options.max_load_date

			
			





user,password,host,src_database,src_table,target_table,\
isPartitioned,partition_column,impalaHost,impalaPort,target_stg_database,target_parquet_db,\
logFolder,min_load_date,max_load_date = argumentParser()

if max_load_date is  None:
	max_load_date=''
if min_load_date is  None:
	min_load_date=''





localtime = time.asctime(time.localtime(time.time()))
log=''
log = log + '\nPython Validator ran at  = '+ str(localtime)



#Checking  Mysql 
config = {'user': user, 'password': password, 'host': host,'database': src_database}  
cnx =mysql.connector.MySQLConnection(**config)
if isPartitioned == True:
	query = "SELECT COUNT(*) FROM "+ src_database +"."+src_table +" AS T WHERE T."+ partition_column+" BETWEEN '"+ min_load_date +"' AND '"+ max_load_date +"';"
else:
	query = "SELECT COUNT(*) FROM " + src_database +"."+src_table +";"

cursor = cnx.cursor()
cursor.execute(query)          
total_mysql_rows = cursor.fetchone()[0]   
#f.write('\nTotal mysql rows  = ' + str(total_mysql_rows) ) 
log = log +'\nTotal mysql rows  = ' + str(total_mysql_rows)



#Checking Impala  :
conn = connect(host=impalaHost, port=impalaPort)
cur = conn.cursor()

if isPartitioned == True:
	query = "SELECT COUNT(*) FROM "+ target_parquet_db +"."+target_table +" AS T WHERE T."+ partition_column+" BETWEEN '"+ min_load_date +"' AND '"+ max_load_date +"';"
else:
	query = "SELECT COUNT(*) FROM " + target_parquet_db +"."+target_table +";"



cur.execute("invalidate metadata;")

cur.execute (query)
result_array=cur.fetchall()
total_impala_rows =result_array[0][0]
log = log +'\nTotal impala rows  = ' + str(total_impala_rows) 

log= '"'+log + '"'

logFilePath =logFolder +'/log.txt'

_command = "echo " + log + ">>" + logFilePath  # saving  log to local


subprocess.Popen(_command,shell=True)"""

