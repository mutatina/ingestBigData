
""" 
Main program that runs other modules needed for ingestion.
CLI arguments : [csv property file ]
To run this on shell : python ingest.py filepath_of_csv_Variables_file, varibleFile, optional[minimumLoad,MaxLoad]
example python ingest.py config.csv config.txt 2017-01-25 2017-01-26
"""


import sys
from IsarLibrary import getmeta,createhql
from myLibrary import getTables,createPropertyfile,createTableInHive,runIngestWorkflow

print "These are the arguments provided: " , sys.argv[1:]
csvFile = sys.argv[1]
varFile=sys.argv[2]
minload=''
maxload=''
if len(sys.argv) > 3:
	minload=sys.argv[3]
	maxload=sys.argv[4]



listOfTablesInfo=getTables(csvFile)

for table in  listOfTablesInfo:
	getmeta(table['src_database'],table['src_table'],table['src_user'],table['src_pwd'],table['src_host'])
	createhql(table['src_database'] ,table['src_table'],table['partitioned_tbl'] ,table['partition_column'])
	createPropertyfile(table,varFile,minload,maxload)
	"""createTableInHive(table['src_table'])
	runIngestWorkflow(table['src_table'])
	
	#checkForCompletion"""
    
	
	




































#oozie jobs -oozie http://localhost:11000/oozie -localtime -len 5 -filter status=RUNNING
	
	
