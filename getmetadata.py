import sys
import csv
import mysql.connector

def getmeta(db_name, table_name):

	# save the file as csv
	#define mysql connection
	config = {'user': 'root', 'password': 'cloudera', 'host': '127.0.0.1','database': 'information_schema'}  
	#table_name = sys.argv[1]
	cnx =mysql.connector.MySQLConnection(**config)
	#cnx = MySQLdb.connect("localhost","root","cloudera","information_schema")
	#define the cusrsor
	cursor = cnx.cursor()
	# select the metadata - 
	#table name should be passed from the customer.properties file, i assume you will create one
	#table_schema should be passed from customer.properties file, i assume you will create one 
	query_start = ("select column_name,data_type,ifnull(character_maximum_length,0) from columns where table_name = ")
	query_end = (" and table_schema= ")
	query = query_start + "'" + table_name + "'" + query_end + "'" + db_name + "'"
	cursor.execute(query)
	result=cursor.fetchall()
	#file path is an optional parameter, if you want to keep it as an in parameter your choice
	# "customers".csv is a variable ie. parameter passed to this program in quotes 
	#print(query)
	out = "/home/cloudera/workspace/" + table_name + ".csv"
	outputFile = csv.writer(open(out, 'wb'))

	for row in result:
		outputFile.writerow(row)
	return

