from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="kafka-spark-hbase")


hadoopConf = sc._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

aws = []
with open('aws_credentials','r') as f:
    for line in f:
            aws.append(line.strip())

hadoopConf = sc._jsc.hadoopConfiguration()

keyId = aws[0]
accessKey = aws[1]

hadoopConf.set("fs.s3a.access.key", keyId)
hadoopConf.set("fs.s3a.secret.key", accessKey)
hadoopConf.set("fs.s3a.fast.upload", "true")     
hadoopConf.set("fs.s3a.buffer.dir", "/tmp")  

sc.setLogLevel('OFF')

ssc = StreamingContext(sc, 10)
zkQuorum = '127.0.0.1:2181'
all_topics  = ['word_counting_upstream_data',
    'word_counting_historical_data',
    'ml_upstream_data',
    'ml_historical_data']
from uuid import uuid1

topics = all_topics 
conf = {'kafka.auto.offset.reset':'smallest','auto.offset.reset':'smallest'}
groupid = str(uuid1())
print(groupid)

#streams = [KafkaUtils.createStream(ssc, zkQuorum, groupid, dict([(x,1) for x in [topic]]),kafkaParams=conf) for topic in topics]

from json import loads,dumps


#hbase_host_zq = '127.0.0.1:2181'
#
#conf = {"hbase.zookeeper.quorum": hbase_host_zq, 
#"hbase.mapreduce.inputtable": 'historical_reddit',
#"hbase.mapreduce.scan":'CgYKAWQSAW04AUAB'
#}
#
#keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
#valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
#
#import json
#
#hbase_rdd = sc.newAPIHadoopRDD(
#    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
#    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
#    "org.apache.hadoop.hbase.client.Result",
#    keyConverter=keyConv,
#    valueConverter=valueConv,
#    conf=conf)


def trCategFeatures(pair):
    features,targets = pair
    title_len,title_num_tokens,body_len,body_num_tokens,post_month,post_weekday,post_over18 = features
    post_month -= 1
    post_weekday -= 1
    post_over18 = int(post_over18)
    features = [title_len,title_num_tokens,body_len,body_num_tokens,post_month,post_weekday,post_over18]
    return [features,targets]
#
#hbase_records = hbase_rdd.mapValues(
#    lambda x: loads(loads(x)['value'])
#).mapValues(
#    trCategFeatures
#)

from functools import partial

def h_to_lpoints(tnum,hrecord):
    k,v = hrecord
    return   LabeledPoint(v[1][tnum],v[0])

#data_rdds = [
#hbase_records.map(partial(h_to_lpoints,0)),
#hbase_records.map(partial(h_to_lpoints,1)),
#hbase_records.map(partial(h_to_lpoints,2))
#]

from pyspark.mllib.tree import GradientBoostedTrees,GradientBoostedTreesModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import RegressionMetrics

models = []

#for data in data_rdds:
#    train, test = data.randomSplit([0.7, 0.3])
#    model = GradientBoostedTrees.trainRegressor(train,{},loss='logLoss',numIterations=1,maxDepth=1)
#    ltest,ftest = test.map(lambda x:x.label),test.map(lambda x: x.features)
#    ptest = model.predict(ftest)
#    
#    for p,l in ptest.zip(ltest).collect():
#        print(f'{p} , has {l}\n')
#    
#    metrics = RegressionMetrics(ptest.zip(ltest))
#    mets = ['meanAbsoluteError','rootMeanSquaredError']
#    for i in mets:
#        print(i,getattr(metrics,i))
#    models.append(model)
#
#for i,model in enumerate(models):
#    path = f's3a://20-04-17-spark/asoetnhu{i}'
#    print(path)
#    model.save(sc,path)

for i in range(3):
    path = f's3a://20-04-17-spark/asoetnhu{i}'
    model = GradientBoostedTreesModel.load(sc,path)
    models.append(model)
    

#w_hdata1 = streams[1] 
#w_hdata1 =  KafkaUtils.createStream(ssc, zkQuorum, groupid, dict([(x,1) for x in [topics[1]]]),kafkaParams=conf)
#def tr1(pair):
#    from re import compile,split
#    reg = compile('[^\w\d]')
#    
#    _,jstr = pair
#    data = [subreddit,sec_to_day,postid,post_title,post_body] = loads(jstr)
#    tokens = reg.split((post_title+' '+post_body).lower())
#    tokenized = []
#    for token in tokens:
#        if not token:
#            continue
#        k = f'W{subreddit}{sec_to_day:0>15}{token}'
#        v = 1
#        tokenized.append((k,v))
#    return tokenized
#
#def tr2(v1,v2):
#    return v1+v2
#
#def tr03(pair):
#    k,amount = pair
#    word = k[17:]
#    cfq1 = 'd:c'
#    v1 = amount
#    cfq2 = 'd:w'
#    v2 = word
#    return (k,dumps([cfq1,v1,cfq2,v2]))
# 
#w_hdata = w_hdata1.flatMap(
#    tr1
#).reduceByKey(
#    tr2
#).map(
#    tr03
#)
#w_hdata.count().pprint(1000)
#
##m_hdata1 = streams[3]
#m_hdata1 =  KafkaUtils.createStream(ssc, zkQuorum, groupid, dict([(x,1) for x in [topics[3]]]),kafkaParams=conf)
#
#def tr3(pair):
#    _,jstr = pair
#    data = [subreddit,sec_to_day,postid,post_title,
#        link_to_post,features,targets] = loads(jstr)
#    k = f'M{subreddit}{sec_to_day:0>15}{postid}'
#    cfq1 = 'd:m'
#    v1 = [features,targets]
#    cfq2 = 'd:g'
#    v2 = [post_title,link_to_post]
#    return (k,dumps([cfq1,v1,cfq2,v2]))
#
#m_hdata = m_hdata1.map(
#    tr3
#)
#m_hdata.count().pprint(1000)
#
#from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
#from pyspark.mllib.util import MLUtils
#features = [1,1,1,1,1234,1,1,True]
#targets = [1,1,1]
#from pyspark.mllib.regression import LabeledPoint
#p = LabeledPoint(targets[0],features)
#data = sc.parallelize([p])
#model = GradientBoostedTrees.trainRegressor(data,categoricalFeaturesInfo={},numIterations=3)
#models = [model,model,model]
##a.save(sc,'tmp/model')
##a.load(sc,'tmp/model')

m_udata1 =  KafkaUtils.createStream(ssc, zkQuorum, groupid, dict([(x,1) for x in [topics[2]]]),kafkaParams=conf)
#m_udata1 = streams[2]
def tr4(pair):
    _,jstr = pair
    data = [subreddit,sec_to_day,postid,post_title,
        link_to_post,features,targets] = loads(jstr)
    k = f'M{subreddit}{sec_to_day:0>15}{postid}'
    cfq1 = 'd:m'
    v1 = [features,targets]
    cfq2 = 'd:g'
    v2 = [post_title,link_to_post]
    return (k,dumps([cfq1,v1,cfq2,v2]))
m_udata2 = m_udata1.map(
    tr4
)
def tr5(pair):
    k,jstr = pair
    cfq1,v1,cfq2,v2 = loads(jstr)
    features,targets = v1
    return features

def trCategFeatures(features):
    features
    title_len,title_num_tokens,body_len,body_num_tokens,post_month,post_weekday,post_over18 = features
    post_month -= 1
    post_weekday -= 1
    post_over18 = int(post_over18)
    features = [title_len,title_num_tokens,body_len,body_num_tokens,post_month,post_weekday,post_over18]
    return features

def tr7(pair):
    k,jstr = pair
    return k

def tr6(rdd):
    keys = rdd.map(tr7)
    features = rdd.map(tr5).map(trCategFeatures)
    preds = [model.predict(features) for model in models]
    z = rdd.join(keys.zip(preds[0].zip(preds[1]).zip(preds[0])))
    return z

def tr8(pair):
    k,(jstr,((t1,t2),t3)) = pair
    data = loads(jstr)
    predicted = [t1,t2,t3]
    data += ['d:p',predicted]
    return (k,dumps(data))
m_udata = m_udata2.transform(tr6).map(
    tr8
)
m_udata.pprint(10)
m_udata.count().pprint()

w_udata1 =  KafkaUtils.createStream(ssc, zkQuorum, groupid, dict([(x,1) for x in [topics[0]]]),kafkaParams=conf)
#w_udata1 = streams[0] 
def tr9(pair):
    from re import compile,split
    reg = compile('[^\w\d]')
    
    _,jstr = pair
    data = [subreddit,sec_to_day,postid,post_title,post_body] = loads(jstr)
    tokens = reg.split((post_title+' '+post_body).lower())
    tokenized = []
    for token in tokens:
        if not token:
            continue
        k = f'W{subreddit}{sec_to_day:0>15}{token}'
        v = 1
        tokenized.append((k,v))
    return tokenized
def tr10(v1,v2):
    return v1+v2
def tr11(pair):
    k,amount = pair
    word = k[17:]
    cfq1 = 'd:c'
    v1 = amount
    cfq2 = 'd:w'
    v2 = word
    return (k,dumps([cfq1,v1,cfq2,v2]))
w_udata = w_udata1.flatMap(
    tr9
).reduceByKey(
    tr10
).map(
    tr11
)
w_udata.pprint(10)
w_udata.count().pprint()

##---------------------------------------
hbase_host_port = '127.0.0.1:2181'

#def save_m_hdata(iter,hbase_table = 'historical_reddit'):
#    import py4j.java_gateway
#    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/root/app_for_reddit/dependencies/*')
#    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/tmp/dependencies/*')
#    jvm = _gateway.jvm
#    imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration']
#    imports.extend(['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get'.split(',')])
#    for entry in imports:
#        py4j.java_gateway.java_import(jvm,entry)
#    hconf = jvm.HBaseConfiguration.create()
#    hconf.set('hbase.zookeeper.quorum',hbase_host_port)
#    connection = jvm.ConnectionFactory.createConnection(hconf);
#    admin = connection.getAdmin();
#    def toBytes(obj):
#        return jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)
#    
#    def createPut(key,family,column,value):
#        theput = jvm.Put(toBytes(key))
#        theput.add(toBytes(family),toBytes(column),toBytes(value))
#        return theput
#    
#    table = connection.getTable(hbase_table) 
#    table.setAutoFlush(True)
#    from json import loads,dumps
#    for key,entry_json  in iter:   
#        cfq1,v1,cfq2,v2 = loads(entry_json)
#        cf1,q1 = cfq1.split(':',maxsplit=1)
#        cf2,q2 = cfq2.split(':',maxsplit=1)
#        put = createPut(key,cf1,q1,dumps(v1))
#        table.put(put)
#        put._detach()
#        put = createPut(key,cf2,q2,dumps(v2))
#        table.put(put)
#        put._detach()
#    connection.close()
#    _gateway.shutdown()
#
#m_hdata.foreachRDD(lambda x: x.foreachPartition(save_m_hdata))

def save_m_udata(iter,hbase_table = 'upstream_reddit'):
    import py4j.java_gateway
    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/root/app_for_reddit/dependencies/*')
    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/tmp/dependencies/*')
    jvm = _gateway.jvm
    imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration']
    imports.extend(['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get'.split(',')])
    for entry in imports:
        py4j.java_gateway.java_import(jvm,entry)
    hconf = jvm.HBaseConfiguration.create()
    hconf.set('hbase.zookeeper.quorum',hbase_host_port)
    connection = jvm.ConnectionFactory.createConnection(hconf);
    admin = connection.getAdmin();
    def toBytes(obj):
        return jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)
    
    def createPut(key,family,column,value):
        theput = jvm.Put(toBytes(key))
        theput.add(toBytes(family),toBytes(column),toBytes(value))
        return theput
    
    table = connection.getTable(hbase_table) 
    table.setAutoFlush(True)
    from json import loads,dumps
    for key,entry_json  in iter:   
        cfq1,v1,cfq2,v2,cfq3,v3 = loads(entry_json)
        cf1,q1 = cfq1.split(':',maxsplit=1)
        cf2,q2 = cfq2.split(':',maxsplit=1)
        cf3,q3 = cfq3.split(':',maxsplit=1)
        put = createPut(key,cf1,q1,dumps(v1))
        table.put(put)
        put._detach()
        put = createPut(key,cf2,q2,dumps(v2))
        table.put(put)
        put._detach()
        put = createPut(key,cf3,q3,dumps(v3))
        table.put(put)
        put._detach()
    connection.close()
    _gateway.shutdown()

m_udata.foreachRDD(lambda x: x.foreachPartition(save_m_udata))

#
#def save_w_hdata(iter,hbase_table = 'historical_reddit'):
#    import py4j.java_gateway
##    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/root/app_for_reddit/dependencies/*')
#    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/tmp/dependencies/*')
#    jvm = _gateway.jvm
#    imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration']
#    imports.extend(['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get,Increment'.split(',')])
#    for entry in imports:
#        py4j.java_gateway.java_import(jvm,entry)
#    hconf = jvm.HBaseConfiguration.create()
#    hconf.set('hbase.zookeeper.quorum',hbase_host_port)
#    connection = jvm.ConnectionFactory.createConnection(hconf);
#    admin = connection.getAdmin();
#    def toBytes(obj):
#        return jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)
#    
#    def createPut(key,family,column,value):
#        theput = jvm.Put(toBytes(key))
#        theput.add(toBytes(family),toBytes(column),toBytes(value))
#        return theput
#    def createIncr(key,family,column,amount):
#        theincr = jvm.Increment(toBytes(key))
#        theincr.addColumn(toBytes(family),toBytes(column),jvm.Long.valueOf(amount))
#        return theincr
#
#    table = connection.getTable(hbase_table) 
#    table.setAutoFlush(True)
#    from json import loads,dumps
#    for key,entry_json  in iter:    
#        cfq1,v1,cfq2,v2 = loads(entry_json)
#        cf1,q1 = cfq1.split(':',maxsplit=1)
#        cf2,q2 = cfq2.split(':',maxsplit=1)
#        incr = createIncr(key,cf1,q1,v1)
#        table.increment(incr)
#        incr._detach()
#        put = createPut(key,cf2,q2,dumps(v2))
#        table.put(put)
#        put._detach()
#    connection.close()
#    _gateway.shutdown()
#
#w_hdata.foreachRDD(lambda x: x.foreachPartition(save_w_hdata))

def save_w_udata(iter,hbase_table = 'upstream_reddit'):
    import py4j.java_gateway
#    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/root/app_for_reddit/dependencies/*')
    _gateway = py4j.java_gateway.JavaGateway().launch_gateway(classpath='/tmp/dependencies/*')
    jvm = _gateway.jvm
    imports = [ 'org.apache.hadoop.hbase.HBaseConfiguration']
    imports.extend(['org.apache.hadoop.hbase.client.'+e for e in 'Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get,Increment'.split(',')])
    for entry in imports:
        py4j.java_gateway.java_import(jvm,entry)
    hconf = jvm.HBaseConfiguration.create()
    hconf.set('hbase.zookeeper.quorum',hbase_host_port)
    connection = jvm.ConnectionFactory.createConnection(hconf);
    admin = connection.getAdmin();
    def toBytes(obj):
        return jvm.org.apache.hadoop.hbase.util.Bytes.toBytes(obj)
    
    def createPut(key,family,column,value):
        theput = jvm.Put(toBytes(key))
        theput.add(toBytes(family),toBytes(column),toBytes(value))
        return theput
    def createIncr(key,family,column,amount):
        theincr = jvm.Increment(toBytes(key))
        theincr.addColumn(toBytes(family),toBytes(column),jvm.Long.valueOf(amount))
        return theincr

    table = connection.getTable(hbase_table) 
    table.setAutoFlush(True)
    from json import loads,dumps
    for key,entry_json  in iter:    
        cfq1,v1,cfq2,v2 = loads(entry_json)
        cf1,q1 = cfq1.split(':',maxsplit=1)
        cf2,q2 = cfq2.split(':',maxsplit=1)
        incr = createIncr(key,cf1,q1,v1)
        table.increment(incr)
        incr._detach()
        put = createPut(key,cf2,q2,dumps(v2))
        table.put(put)
        put._detach()
    connection.close()
    _gateway.shutdown()

w_udata.foreachRDD(lambda x: x.foreachPartition(save_w_udata))



#--------------------------------------

ssc.start()
ssc.awaitTermination()
