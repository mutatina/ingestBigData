import subprocess,csv,urllib2,json,time
import sys,csv,mysql.connector
from datetime import datetime,timedelta

from impala.dbapi import connect



def divideDates(minload,maxload,dateRange):
	"""
	Give dates like  from "2016-12-25" to "2017-02-27": with date range :10

	this function returns a list of  dictionaries conatining 5 days range like
	[{'maxload': '2017-01-04', 'minload': '2016-12-25'}, {'maxload': '2017-01-14', 'minload': '2017-01-05'},
	 {'maxload': '2017-01-24', 'minload': '2017-01-15'}, {'maxload': '2017-02-03', 'minload': '2017-01-25'},
         {'maxload': '2017-02-13', 'minload': '2017-02-04'}, {'maxload': '2017-02-23', 'minload': '2017-02-14'}, 
	 {'maxload': '2017-02-27', 'minload': '2017-02-24'}]
	"""

	mindate=datetime.strptime(minload, '%Y-%m-%d').date()
	maxload=datetime.strptime(maxload, '%Y-%m-%d').date()
	listOfdates=[]	
	maxdate= mindate
	while mindate <= maxload:
    		maxdate += timedelta(dateRange)
		if maxdate > maxload:
			maxdate =maxload
    		listOfdates.append({'minload':mindate.strftime("%Y-%m-%d"),'maxload':maxdate.strftime("%Y-%m-%d")})
    		mindate = maxdate + timedelta(1)
    	return listOfdates

def createPropertyfile(tableInfoDictionary,varFile,minload,maxload):
	"""
	Name:createPropertyfile
	Description:This function creates .properties file using the CSV and TXT files	given along with  min and max load-dates if available.
	Parameters:config text file path, min and max load Dates, infomation about the mysql table
	"""
	
	#get required info from varFile

	readFile= open(varFile,'r')
	file_str= readFile.read()
	readFile.close()
	
	hqlScriptStart= file_str.find('hqlScriptFiles')+15
	hqlScriptEnd = hqlScriptStart+file_str[hqlScriptStart:].find('\n')
	hqlScriptFiles =file_str[hqlScriptStart:hqlScriptEnd]
	
	hqlScriptDict= eval(hqlScriptFiles)
	hqlScriptFile=""
	if eval(tableInfoDictionary['partitioned_tbl']):
		
		if minload and maxload:   #If dates have been provided
			#print 'Dates have been provided'
			hqlScriptFile= hqlScriptDict['hqlFilePartRange']
		else:	
			#print 'No dates provided ,using script for loading ALL partitioned table'
			hqlScriptFile=	hqlScriptDict['hqlFilePart']
			minload='NA'
			maxload='NA'

	else: 
		#print "Non partitioned table detected going to use script for not partitioned table"
		hqlScriptFile=hqlScriptDict['hqlFileNonPart']
		minload='NA'
		maxload='NA'

	fileStrWithoutHql=file_str.replace(file_str[hqlScriptStart-15:hqlScriptEnd],'').strip()
	

	#preparing to add the elements in the file
	requiredString=fileStrWithoutHql +'\n'
	requiredString=requiredString + "min_load_date="+ minload +'\n'
	requiredString=requiredString + "max_load_date="+ maxload +'\n'
	requiredString=requiredString + "hqlScriptFile="+ hqlScriptFile +'\n'
	
	

        outputFileString =tableInfoDictionary['target_table']+".properties"
	outputFile= open(outputFileString,'w')
	outputFile.write(requiredString)
	#writing everything from the csv file

	for key,value in tableInfoDictionary.iteritems():
       		outputFile.write(key +'='+ value +'\n')	
	outputFile.close()
	print 'Properties file',outputFileString,'generated for the dates' ,minload,'to',maxload
	return



def createTableInHive(table_name):
	"""
	Name:createTableInHive
	:This function creates and executes Hive commands to create tables in Hive using the HQLs.	
	Parameters: table_name
	"""

	stageHqlfileLocation = '/home/cloudera/hive/create_' + table_name + '_stg.hql'
	parquetHqlfileLocation ='/home/cloudera/hive/create_' + table_name + '_parquet.hql'

	fileList=[stageHqlfileLocation,parquetHqlfileLocation]
	for _file in fileList:
		hiveCommand = "hive -f " + _file
		
		try:
			subprocess.check_call(hiveCommand,shell=True)
		except subprocess.CalledProcessError, e:
			print ("ERROR :Could not create the Table with "+_file)
			
		else:
			print "Table created succesfully"

	return

def runIngestWorkflow(table_name,jobsDict,minload,maxload):
	"""
	Name:runIngestWorkflow
	Description:This function runs the oozie command  for the given table  and reports the results in a success/fail dictionary given.
	Parameters:Name of the table to be ingested and a dictionary to record results.
	"""

	propertiesFileLocationPath = table_name + ".properties"
	
	oozieCommand =	"oozie job -oozie http://localhost:11000/oozie -config "+propertiesFileLocationPath +" -run"
	
		
	p=subprocess.Popen(oozieCommand,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE)
	
	error_output= p.stderr.readline()
	success_output = p.stdout.readline()
		
	if error_output:
			
		print ("ERROR Could not run the oozie workflow for table :"+ table_name + ' from ' +minload+' to '+maxload)
		print('Details:',error_output)
		jobsDict['fail'][table_name + ' from ' +minload+' to '+maxload]=error_output

	if success_output:
		oozieJobID = success_output[success_output.find(':')+1:success_output.find('\n')].strip()	
		print ("Ozzie job succesfully submitted for table : "+ table_name + ' from ' +minload+' to '+maxload)
		print ('oozieJobID ',oozieJobID)
		jobsDict['success'][table_name + ' from ' +minload+' to '+maxload]=oozieJobID

	return








def getTables(csvFile):
	"""
	Name:getTables
	Description:Given the csv file location this function returns a list of tables found in the file.	
	Parameters:csv file location path
	"""
	tableList=[]
	with open(csvFile) as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			tableList.append(row)  
	return tableList


def getJobStatus(jobsDict):
	"""
	Name:getJobStatus
	Description: This function checks and report the status of succesfully submited oozie jobs in the dictionary provided,
			 and reports the 	finalresults back to the dictionary.	
	Parameters:Dictionary of success and failed jobs
	"""
	URL_start = 'http://localhost:11000/oozie/v1/job/'
	URL_finish = '?show=info&timezone=GMT'
	
	numberOfJobs=len(jobsDict['success'])
	jobsCompleted = 0

	while (jobsCompleted < numberOfJobs):
		for table,status in jobsDict['success'].iteritems():
			if status not in ['FAILED','KILLED','TIMEDOUT','SUCCEEDED']:
				
							
				Target_job =status
				FULL_URL = URL_start + Target_job + URL_finish
				req = urllib2.Request(FULL_URL)
				response = urllib2.urlopen(req)
				outputDict = json.loads(response.read())

				if (outputDict['status'] =='SUCCEEDED'):				
					
					jobsDict['success'][table]='SUCCEEDED'
					jobsCompleted = jobsCompleted + 1
					print'Table: '+table+ ' has successfully been ingested with status: '+outputDict['status']
					
					

				elif outputDict['status'] in ['FAILED','KILLED','TIMEDOUT']:
					print 'Table: '+table+ ' has FAILED ingestion with status '+outputDict['status']
					jobsCompleted = jobsCompleted + 1		
					jobsDict['fail'][table]=outputDict['status']
					jobsDict['success'][table]=outputDict['status']
					
					
				else:	
					print 'Current ingestion status for table: '+ table  +' is ' + outputDict['status']
				
					time.sleep(5)
						

	return



def runAggregationWorkflow():
	"""
	Name:runAggregationWorkflow
	Description:
	"""
	
	oozieCommand =	"oozie job -oozie http://localhost:11000/oozie -config aggregation.properties -run"
	
		
	p=subprocess.Popen(oozieCommand,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE)
	
	error_output= p.stderr.readline()
	success_output = p.stdout.readline()
		
	if error_output:
			
		print('Error Details:',error_output)
		

	if success_output:
			
		print ("Ozzie job was succesfully submitted!")	
	
	return


def Get_Last_Partition_Date(table_name):
#
#   Given a table name, returns "Next_partition_date" formatted as a string to be stuffed back into a new job.properties for that table
#	
#	print('entering Get_Last_Partition_Date')
	one_day = timedelta(days = 1)
#	two_days = timedelta(days = 2)
	conn = connect(host='localhost', port=21050)
	cursor = conn.cursor()
	exec_str = 'SELECT max(trans_date) maximpaladate FROM retail2.' + table_name
	cursor.execute(exec_str)
	load_min_date = cursor.fetchone()
	load_min_date =load_min_date[-1].strip()
#	print 'last_load_date=' + load_min_date
#	print (type(load_min_date))
	date_temp = datetime.strptime(load_min_date, '%Y-%m-%d')
	d = datetime.date(date_temp)
	Next_partition_temp = d + one_day
	Next_partition_date = datetime.strftime(Next_partition_temp,'%Y-%m-%d')
	#print ('Reformatted date is now ', Next_partition_date)
	currentDate =datetime.now().strftime('%Y-%m-%d')

	return (Next_partition_date,currentDate)



def cleanPartitionCol(table_name,partition_col):
	"""
	Name:cleanPartitionCol
	Description: This handy utitlity  cleans/removes the extra partition-column line in HQL files:	
	Parameters:table name and the partition column to be removed in the hql file
	"""
	
	parquetHqlfileLocation ='/home/cloudera/hive/create_' + table_name + '_parquet.hql'

	fileList=[parquetHqlfileLocation]
	for _file in fileList:
		f = open(_file)
		file_str = f.read() 
		f.close()
		file_str_temp=file_str.lower()
		
		extraLineStarts= file_str_temp.find(partition_col.lower()) - 1 
		extraLineEnds= extraLineStarts + file_str_temp[extraLineStarts:].find('\n')  + 1
		
		extraLine =file_str_temp[extraLineStarts:extraLineEnds]
		print "Removing: "+ extraLine + 'from file '+ _file
		file_str=file_str.replace(file_str[extraLineStarts:extraLineEnds],"")
	
		f = open(_file, 'w') 
		f.write(file_str)
		f.close()


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


