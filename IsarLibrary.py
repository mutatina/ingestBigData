import sys,csv,mysql.connector
from myLibrary import cleanPartitionCol
def createhql(stage_db,parquet_db,table_name, partition_string, partition_col):
	#variable definition
	#read and write directory,file name(create+tableName+stg OR +parquet), database name retail_stg, retail
	# table name all all be parameters of this program.
	#variable definition 
	database_name_staging = stage_db
	database_name_pqt = parquet_db
	partition =eval(partition_string)
	#partition =True
	#partition_col = "part_col"
	#table_name = "order_detail"
	fileToRead='/home/cloudera/workspace/' + table_name + '.csv'
	stgFileToWrite ='/home/cloudera/hive/create_' + table_name + '_stg.hql'
	pqtFileToWrite ='/home/cloudera/hive/create_' + table_name + '_parquet.hql'
	linenum =0
	stgLineToWriteDropTableHeader='DROP TABLE IF EXISTS ' + database_name_staging + '.' + table_name + ';' + '\n'
	stgLineToWriteCreateTable='CREATE EXTERNAL TABLE IF NOT EXISTS ' + database_name_staging + '.' + table_name + ' (' + '\n'
	stgLineToWriteDelimit='ROW FORMAT DELIMITED FIELDS TERMINATED BY ' + "'\\001'"  + '\n'
	stgLineToWriteDelimit2='LINES TERMINATED BY ' + "'\\n'" + '\n'
	stgLineToWriteFileformat='STORED AS TEXTFILE;'  + '\n'
	#stgLineToWritePartition='PARTITIONED BY (`' + partition_col + '` date)' + '\n'
	pqtLineToWriteDropTableHeader='DROP TABLE IF EXISTS ' + database_name_pqt + '.' + table_name + ';' + '\n'
	pqtLineToWriteCreateTable='CREATE EXTERNAL TABLE IF NOT EXISTS ' + database_name_pqt + '.' + table_name + ' (' + '\n'
	pqtLineToWriteStorage='STORED AS PARQUET'  + '\n'
	pqtLineToWritePartition='PARTITIONED BY (`' + partition_col + '` string)' + '\n'
	pqtLineToWriteCompress='TBLPROPERTIES("parquet.compression"="SNAPPY");'  + '\n'
	#pqtLineToWriteDelimit='ROW FORMAT DELIMITED FIELDS TERMINATED BY ' + "'\\001'"  + '\n'
	#pqtLineToWriteDelimit2='LINES TERMINATED BY ' + "'\\n'" + '\n'
	lineToWriteClose=')'  + '\n'
	#file processing
	with open(fileToRead, 'r') as readFile:
		stgwriteFile= open(stgFileToWrite, 'w') #open staging file to write
		pqtwriteFile= open(pqtFileToWrite, 'w') #open parquet file to write
	#Start writing to files
		stgwriteFile.write(stgLineToWriteDropTableHeader)
		
		stgwriteFile.write(stgLineToWriteCreateTable)
		pqtwriteFile.write(pqtLineToWriteDropTableHeader)
		pqtwriteFile.write(pqtLineToWriteCreateTable)
	#iterate the records in file, 
	#we are converting tinyint to int
	#date  will be converted to string
	# for now any other type will be converted to string, that is me you dont have to do it. 
	#You will have to figure out other types. To convert or not to convert. 
		for line in readFile :
			linenum ==linenum
			columnnm, columnDataType, columnLength = line.split(',')
			if linenum ==0: #if this is the first line(column read from input file for creating table, dont put a comma start of line
				if columnDataType.lower() =='tinyint':
				   columnDataType='int'
				   lineToWrite=columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='float':
				   columnDataType='decimal (8,2)'
				   lineToWrite=columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='date':
				   lineToWrite=columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='date':
				   columnDataType='string'
				   lineToWrite=columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='int':
				   columnDataType='int'
				   lineToWrite=columnnm +' ' + columnDataType + '\n'
				else: 
					columnDataType='string'	       
					lineToWrite=columnnm +' ' + columnDataType + '\n'  
			else:
				if columnDataType.lower() =='tinyint':
					columnDataType='int'
					lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='float':
				   columnDataType='decimal (8,2)'
				   lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='datetime':
				   columnDataType='string'
				   lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='date':
				   columnDataType='string'
				   lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='varchar':
				   columnDataType='string'
				   lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				elif columnDataType.lower() =='int':
				   columnDataType='int'
				   lineToWrite=','+columnnm +' ' + columnDataType + '\n'
				else: 
					columnDataType='string'	       
					lineToWrite=','+columnnm +' ' + columnDataType + '\n'
			stgwriteFile.write(lineToWrite) #writing each columns
			pqtwriteFile.write(lineToWrite)
			linenum =linenum + 1
		stgwriteFile.write(lineToWriteClose)
		stgwriteFile.write(stgLineToWriteDelimit)
		stgwriteFile.write(stgLineToWriteDelimit2)
		stgwriteFile.write(stgLineToWriteFileformat)
		stgwriteFile.close() #file creation complete close the file
		pqtwriteFile.write(lineToWriteClose)
		if partition ==True:
			print('Partition is true for parquet table')
			pqtwriteFile.write(pqtLineToWritePartition)
		pqtwriteFile.write(pqtLineToWriteStorage)
		pqtwriteFile.write(pqtLineToWriteCompress)
		pqtwriteFile.close() #file creation complete close the file
		if partition ==True:
			cleanPartitionCol(table_name,partition_col)
	return



def getmeta(db_name, table_name,user,password,mySqlHost):

	# save the file as csv
	#define mysql connection
	config = {'user': user, 'password': password, 'host': mySqlHost,'database': 'information_schema'}  
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


