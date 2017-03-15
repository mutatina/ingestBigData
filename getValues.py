# Given a key this function returns a list[] of values found in the .properties file.
#If key is not found it will return a boolean False value.
#assumption : Unique Keys exist in file.
def fetch(key):
	
	with open("ingest.properties") as properties:
		for line in properties:
            		if key in line:
				equalSignIndex = line.find('=')
				newLineIndex=line.find('\n')
				values=line[equalSignIndex +1:newLineIndex]
                		valueList = values.split()
                		return valueList
			
		return False
