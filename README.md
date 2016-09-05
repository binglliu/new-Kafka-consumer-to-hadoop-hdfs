Read messages/records/events from Kafka and same them into Hadoop HDFS is a typical use of Kafka messaging system. 
There are some example codes on internet about this use case, but they all use Kafka version older than 0.8.0. Since Kafka 0.9.0 a new Kafka consumer is introduced, the new Kafka consumer APIs are different from earlier version. for example:
 - it does not need zookeeper list;
 - it does not use fetch API with offset, it uses poll with offset auto-commit...

Something new in my code from others:
 - new kafka consumer APIs used;
 - No need to provide zookeeper list;
 - one mapper/reducer for each partition to dedup duplicated message, newer one will overwrite older one

This code is easy to expand for customizied use.

Usage:
1, start kafka server locally;
2, start hdfs server locally and create your hdfs folders;
3, produce some message to your local kafka server;
4, run hadoop job:
   hadoop jar build/libs/hadoop_hdfs_kafka-1.0-SNAPSHOT.jar hadoop.kafka.newconsumer.HadoopKafkaConsumer -libjars kafka-clients-0.9.0.0.jar:kafka_2.10-0.9.0.0.jar

