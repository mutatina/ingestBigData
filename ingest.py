""" 
Name: ingest.py
Description:Main program that runs other modules needed for ingestion.
	CLI arguments : [csv file ,arg text file ]
	To run this on shell : python ingest.py filepath_of_csv_Variables_file, varibleFile, optional[minimumLoad,MaxLoad]
	Example: python ingest.py config.csv config.txt 2016-12-24 2016-12-24
"""

import sys
from myLibrary import getTables,createPropertyfile,createTableInHive,runIngestWorkflow,getJobStatus,runAggregationWorkflow,getmeta,createhql

#Parsing CLI arguments
csvFile = sys.argv[1]
varFile=sys.argv[2]
minload=''
maxload=''
if len(sys.argv) > 3:
	minload=sys.argv[3]
	maxload=sys.argv[4]



listOfTablesInfo=getTables(csvFile) #List of Mysqly Tables infomation obtained from the csv file
jobsDict={'success':{},'fail':{}}   #Dictionary to track passsed and failed jobs. 


for table in  listOfTablesInfo:  
	getmeta(table['src_database'],table['src_table'],table['src_user'],table['src_pwd'],table['src_host'])
	createhql(table['target_stg_database'],table['target_parquet_db'] ,table['src_table'],table['partitioned_tbl'] ,table['partition_column'])
	createPropertyfile(table,varFile,minload,maxload)
	createTableInHive(table['src_table'])
	runIngestWorkflow(table['src_table'],jobsDict)
	

getJobStatus(jobsDict)
print ('Workflow Results:', jobsDict)
runAggregationWorkflow(jobsDict)




  
