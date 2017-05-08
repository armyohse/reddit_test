#!/usr/bin/python
from subprocess import getoutput,run,PIPE,Popen,STDOUT

from sys import argv
host = 'spark://'+getoutput('hostname -f')+':7077'
deppref = '/root/dependencies'

command_args = '\
spark-submit \
--master '+host+ ' \
--conf="spark.jars=\
'+deppref+'/hbase-common-1.3.0.jar,\
'+deppref+'/spark-core_2.11-2.2.0-SNAPSHOT.jar,\
'+deppref+'/hbase-server-1.3.0.jar,\
'+deppref+'/hbase-client-1.3.0.jar,\
'+deppref+'/guava-12.0.1.jar,\
'+deppref+'/hbase-protocol-1.3.0.jar,\
'+deppref+'/htrace-core-3.1.0-incubating.jar,\
'+deppref+'/metrics-core-2.2.0.jar,\
'+deppref+'/log4j-1.2.17.jar,\
'+deppref+'/minlog-1.3.0.jar,\
'+deppref+'/slf4j-log4j12-1.7.16.jar,\
'+deppref+'/hbase-hadoop2-compat-1.3.0.jar,\
'+deppref+'/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar,\
'+deppref+'/original-spark-examples_2.11-2.2.0-SNAPSHOT.jar,\
'+deppref+'/aws-java-sdk-1.11.121.jar,\
'+deppref+'/hadoop-aws-2.7.3.jar,\
'+deppref+'/py4j-0.10.4-src.zip" \
spark_processing.py ' 

import shlex

import sys
o = Popen([*shlex.split(command_args)], stdout=sys.stdout,stderr=sys.stdout)
o.wait()    
