import subprocess,csv
#Given table infomation in a Dictionary type, this function creates a job.property file.

def createPropertyfile(tableInfoDictionary,varFile,minload,maxload):

	
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
		print minload,maxload
		if minload and maxload:   #If dates have been provided
			print 'Dates have been provided'
			hqlScriptFile= hqlScriptDict['hqlFilePartRange']
		else:	
			print 'No dates provided ,using script for loading ALL partitioned table'
			hqlScriptFile=	hqlScriptDict['hqlFilePart']
			minload='NA'
			maxload='NA'

	else: 
		print "Non partitioned table detected going to use script for not partitined table"
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
	print 'properties file generated: ',outputFileString
	return


# Given table name this function will run the two hql files(stg & parquet) for the table in the hive shell

def createTableInHive(table_name):

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

def runIngestWorkflow(table_name):

	propertiesFileLocationPath = table_name + ".properties"
	#oozieCommand = "export OOZIE_URL=http://localhost:11000/oozie" + "\n" + "oozie job -config "+propertiesFileLocationPath +" -run"
	oozieCommand =	"oozie job -oozie http://localhost:11000/oozie -config "+propertiesFileLocationPath +" -run"
	
		#result=subprocess.check_output(oozieCommand,shell=True)
	p=subprocess.Popen(oozieCommand,shell=True,stderr=subprocess.PIPE,stdout=subprocess.PIPE)
	
	error_output= p.stderr.readline()
	success_output = p.stdout.readline()
		
	if error_output:
			
		print ("ERROR Could not run the oozie workflow for table :"+table_name)
		print('Details:',error_output)

	if success_output:
		oozieJobID = success_output[success_output.find(':')+1:success_output.find('\n')].strip()	
		print ("Ozzie job succesfully submitted for table : "+table_name)
		print ('oozieJobID ',oozieJobID)

	return


# Handy utitlity to clean/remove the extra partition column line in HQL files:
def cleanPartitionCol(table_name,partition_col):
	stageHqlfileLocation = '/home/cloudera/hive/create_' + table_name + '_stg.hql'
	parquetHqlfileLocation ='/home/cloudera/hive/create_' + table_name + '_parquet.hql'

	fileList=[stageHqlfileLocation,parquetHqlfileLocation]
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


def getTables(csvFile):
	tableList=[]
	with open(csvFile) as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			tableList.append(row)  
	return tableList



