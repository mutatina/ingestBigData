
""" 
Main program that runs other modules needed for ingestion.
CLI arguments : [csv property file ]
To run this on shell : python ingest.py filepath_of_csv_Variables_file
"""


import sys
from IsarLibrary import getmeta,createhql
from myLibrary import getTables,createPropertyfile,createTableInHive,runIngestWorkflow

csvFile = str(sys.argv[1])

listOfTablesInfo=getTables(csvFile)

for table in  listOfTablesInfo:
	"""getmeta(table['src_database'],table['src_table'])
	createhql(table['src_database'] ,table['src_table'],table['partitioned_tbl'] ,table['partition_column'])
	createPropertyfile(table)
	createTableInHive(table['src_table'])"""
	runIngestWorkflow(table['src_table'])
	
	#checkForCompletion
    
	
	




































#oozie jobs -oozie http://localhost:11000/oozie -localtime -len 5 -filter status=RUNNING
	
	
