""" 
Name: ingest.py
Description:Main program that runs other modules needed for ingestion.
	CLI arguments : [csv file ,arg text file ]
	To run this on shell : python ingest.py filepath_of_csv_Variables_file, varibleFile, optional[minimumLoad,MaxLoad] dateRangePerJob
	Example: python ingest.py config.csv config.txt 2016-12-24 2016-12-24 10
"""

import sys
from myLibrary import getTables,createPropertyfile,createTableInHive,runIngestWorkflow,getJobStatus,runAggregationWorkflow,getmeta,createhql,divideDates,Get_Last_Partition_Date
from datetime import datetime,timedelta

#Parsing CLI arguments
csvFile = sys.argv[1]
varFile=sys.argv[2]
minload=''
dateRange =7     # 7 days default
if len(sys.argv) > 3:
	minload=sys.argv[3]
	maxload=sys.argv[4]
	dateRange =int(sys.argv[5])



listOfTablesInfo=getTables(csvFile) #List of Mysqly Tables infomation obtained from the csv file
jobsDict={'success':{},'fail':{}}   #Dictionary to track passsed and failed jobs. 


for table in  listOfTablesInfo:   # Prepping the data for ingestion
	getmeta(table['src_database'],table['src_table'],table['src_user'],table['src_pwd'],table['src_host'])
	createhql(table['target_stg_database'],table['target_parquet_db'] ,table['src_table'],table['partitioned_tbl'] ,table['partition_column'])	
	#createTableInHive(table['src_table'])


for table in  listOfTablesInfo:  # creating .properties file and execute ingestion
	if eval(table['partitioned_tbl']) == True:
		if minload=='':
			partitionTableName=table['target_table'] 
			(minload,maxload)=Get_Last_Partition_Date(partitionTableName)
				
		for dates in divideDates(minload,maxload,dateRange):
			createPropertyfile(table,varFile,dates['minload'],dates['maxload'])
			runIngestWorkflow(table['src_table'],jobsDict,dates['minload'],dates['maxload'])			

	else:
		
		createPropertyfile(table,varFile,minload,maxload)
		runIngestWorkflow(table['src_table'],jobsDict,minload,maxload)
	

getJobStatus(jobsDict)


if jobsDict['fail']: 

	print "Can not run Aggregation  workflow , there exists at least one table that failed to move."
	
else:
	print "All submitted jobs succedded, running the aggregation workflow now"
	runAggregationWorkflow()
  
