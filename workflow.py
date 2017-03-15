import urllib2

xml_string="""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>oozie.wf.application.path</name>
        <value>hdfs://quickstart.cloudera:8020/user/cloudera/oozie/workflowPy.xml</value>
    </property>
    <property>
        <name>nameNode</name>
        <value>hdfs://quickstart.cloudera:8020</value>
    </property>
    <property>
        <name>jobTracker</name>
        <value>quickstart.cloudera:8032</value>
    </property>
    <property>
        <name>oozie.use.system.libpath</name>
        <value>true</value>
    </property>  
</configuration>"""




url= 'http://localhost:11000/oozie/v1/jobs?action=start'
data = (xml_string)
headers = {'Content-Type': 'application/xml;charset=UTF-8'}


req = urllib2.Request(url,data,headers)
response = urllib2.urlopen(req)
output = response.read()
print output


#import json
#req = urllib2.Request('http://localhost:11000/oozie/v1/jobs?jobtype=wf')
#response = urllib2.urlopen(req)
#output = response.read()
#print json.dumps(json.loads(output), indent=4, separators=(',', ': '))





