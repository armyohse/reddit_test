import pyspark

if globals().get('sc'):
    sc.stop()
sc = pyspark.SparkContext()

import py4j
imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration','org.apache.hadoop.hbase.util.Bytes']
for e in imports:
    py4j.java_gateway.java_import(sc._jvm,e)
    
imports2 = ['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get'.split(',')]
for e in imports2:
    py4j.java_gateway.java_import(sc._jvm,e)
    
def search(iter,patt):
    from re import search
    matched = []
    for i in iter:
        match = search(pattern=patt,string=i)
        if match:
            matched.append(i)
    return matched
hconf = sc._jvm.HBaseConfiguration.create()
hconf.set('hbase.zookeeper.quorum','localhost:2181')
confstrs = [b.toString() for b in hconf.iterator()]
connection = sc._jvm.ConnectionFactory.createConnection(hconf);
admin = connection.getAdmin();
admin.listTables()
emp = connection.getTable('emp')

host = '127.0.0.1:2181'  
table = 'emp'  
conf = {
    "hbase.zookeeper.quorum": host,  
    "hbase.mapred.outputtable": table,  
    "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",  
    "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.Writeable",  
    "mapreduce.job.output.value.class": "org.apache.hadoop.hbase.client.Put"}  

def toBytes(obj):
    return sc._jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)

def createPut(key,family,column,value):
    theput = sc._jvm.Put(toBytes(key))
    theput.add(toBytes(family),toBytes(column),toBytes(value))
    return (key,theput)

from time import time   
_,theput = createPut(f'key1{time()}','personal data','name','Ned Stark')
emp.put(theput)
thescan = sc._jvm.org.apache.hadoop.hbase.client.Scan()
scanner = emp.getScanner(thescan)
data = []
data.append(scanner.next())
while data[-1]:
    data.append(scanner.next())
data = data[:-1]
datastrs = [e.getMap() for e in data]
from pprint import pprint
pprint(datastrs[:10])
print(len(datastrs))

import os
def worker(iter, pyspark_submit_args = os.environ.get('PYSPARK_SUBMIT_ARGS') or '' ):
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = pyspark_submit_args
    from pyspark.java_gateway import launch_gateway
    import py4j.java_gateway
    _gateway = 'empty'
    if pyspark_submit_args:
        _gateway = launch_gateway() #pyspark shell compatible
    else:
        _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/opt/hbase/lib/*:/opt/apache-spark/assembly/target/scala-2.11/jars/*')
    jvm = _gateway.jvm
    imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration','org.apache.hadoop.hbase.util.Bytes']
    for e in imports:
        py4j.java_gateway.java_import(jvm,e)    
    imports2 = ['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get'.split(',')]
    for e in imports2:
        py4j.java_gateway.java_import(jvm,e)
    hconf = jvm.HBaseConfiguration.create()
    hconf.set('hbase.zookeeper.quorum','localhost:2181')
    confstrs = [b.toString() for b in hconf.iterator()]
    connection = jvm.ConnectionFactory.createConnection(hconf);
    admin = connection.getAdmin();
    admin.listTables()
    emp = connection.getTable('emp')
    def toBytes(obj):
        return jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)

    def createPut(key,family,column,value):
        theput = jvm.Put(toBytes(key))
        theput.add(toBytes(family),toBytes(column),toBytes(value))
        return (key,theput)

    from time import time   
    
    
    emp = connection.getTable('emp') 
    emp.setAutoFlush(True)
    #converter = py4j.java_collections.ListConverter()
    #java_list = converter.convert([],_gateway._gateway_client)
    #java_list.ensureCapacity(1000)
    for entry in iter:
        _,theput = createPut(f'rowkey {entry}','personal data','name',f'Ned Stark will be alive {entry} times')
        #java_list.add(theput)
        emp.put(theput)
        theput._detach()
    #emp.put(java_list)
    
    connection.close()
    if not pyspark_submit_args:
        _gateway.shutdown()        
    else:
        _gateway.shutdown()
    return  
from datetime import datetime

import sys 
start = datetime.now()
testsize = int(sys.argv[1])
result = sc.parallelize([i for  i in range(testsize)]).foreachPartition(worker)
end = datetime.now()
print(start,end,testsize,(end-start).total_seconds())