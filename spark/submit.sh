spark-submit --mode cluster --master spark://localhost:7077\
--conf "spark.jars=/tmp/hbase-common-1.3.0.jar,\
/opt/apache-spark/assembly/target/scala-2.11/jars/spark-core_2.11-2.2.0-SNAPSHOT.jar,\
/opt/hbase/lib/hbase-server-1.3.0.jar,/opt/hbase/lib/hbase-client-1.3.0.jar,\
/opt/hbase/lib/guava-12.0.1.jar,\
/opt/hbase/lib/hbase-protocol-1.3.0.jar,\
/opt/hbase/lib/htrace-core-3.1.0-incubating.jar,\
/opt/hbase/lib/metrics-core-2.2.0.jar,\
hbase-spark-2.0.0-SNAPSHOT.jar,\
/opt/apache-spark/assembly/target/scala-2.11/jars/log4j-1.2.17.jar,\
/opt/apache-spark/assembly/target/scala-2.11/jars/minlog-1.3.0.jar,\
/opt/apache-spark/assembly/target/scala-2.11/jars/slf4j-log4j12-1.7.16.jar,\
../spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar,/tmp/spark-core_2.11-1.5.2.logging.jar,\
/opt/hbase/lib/hbase-hadoop2-compat-1.3.0.jar,original-spark-examples_2.11-2.2.0-SNAPSHOT.jar,\
/opt/apache-spark/python/lib/py4j-0.10.4-src.zip"\ 
--conf "spark.core.max=1" --conf "spark.executor.cores=1" spark_hbase.py 1000
