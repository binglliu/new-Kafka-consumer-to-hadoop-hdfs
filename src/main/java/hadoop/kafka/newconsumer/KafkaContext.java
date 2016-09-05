package hadoop.kafka.newconsumer;


import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaContext implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(KafkaContext.class);

    KafkaConsumer<String, byte[]> consumer;
    String topic;
    int totalPartitions;
    int partition;
    ConsumerRecords<String, byte[]> messages;
    Iterator<ConsumerRecord<String, byte[]>> iterator;
    final ArrayBlockingQueue<ConsumerRecords<String, byte[]>> queue;
    final FetchThread fetcher;

    private Gson gson;

    public KafkaContext(String brokers, String topic, int totalPartitions,
                        int partition, int timeout, int bufferSize) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", topic);
        props.put("client.id", "my_test_kafka_client_name");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("max.partition.fetch.bytes", bufferSize);
        consumer = new KafkaConsumer<String, byte[]>(props);
        gson = new Gson();

        this.topic = topic;
        this.totalPartitions = totalPartitions;
        this.partition = partition;

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(topicPartition);

        queue = new ArrayBlockingQueue<ConsumerRecords<String, byte[]>>(100);
        fetcher = new FetchThread(consumer, queue, topic, partition);
        fetcher.start();
    }

    @Override
    public void close() throws IOException {
        fetcher.stop = true;
        while (!fetcher.stopped);
        consumer.close();
        logger.info("kafkaContext closed.");
    }

    private boolean hasMore() {
        if (iterator == null) {
            fetchMore();
            if (iterator == null) {
                return false;
            }
        }
        boolean hasNext = iterator.hasNext();
        if (hasNext) return hasNext;
        else {
            fetchMore();
            return iterator.hasNext();
        }
    }

    private void fetchMore() {
        while(!fetcher.stop || !queue.isEmpty()) {
            messages = queue.poll();
            if (messages != null) {
                iterator = messages.iterator();
                break;
            }
        }
    }

    public long getNext(LongWritable key, KafkaMessageWithOffset value) throws IOException {
        if ( !hasMore() ) return -1L;
        ConsumerRecord<String, byte[]> message = iterator.next();
        long offset = message.offset();
        key.set(partition);

        MyKafkaMessage msg = null;
        if(message.value() != null) {
            msg = new Gson().fromJson(String.valueOf(message.value()), MyKafkaMessage.class);

            if(msg != null) {
                logger.info("read kadka message id{}, value {}", msg.getId(), msg.getMsg());

                value.setOffset(new LongWritable(offset));
                value.setMessageJson(new Text(new Gson().toJson(msg)));
            } else {
                logger.error("kafka message is null", message.key());
            }
        } else {
            logger.error("Invalid Kafka message {}", message.key());
        }

        return offset;
    }

    static class FetchThread extends Thread {
        String topic;
        int partition;
        KafkaConsumer<String, byte[]> consumer;
        public volatile boolean stop = false;
        public volatile boolean stopped = false;
        ArrayBlockingQueue<ConsumerRecords<String, byte[]>> queue ;

        public FetchThread(KafkaConsumer<String, byte[]> consumer, ArrayBlockingQueue<ConsumerRecords<String, byte[]>> queue,
                           String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
            this.consumer = consumer;
            this.queue = queue;
        }

        @Override
        public void run() {
            stop = false;
            stopped = false;
            while (!stop) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                if (records == null || records.isEmpty()) {
                    logger.info("Done! Received null or empty records when processing partition "+ partition);
                    stop = true;
                    continue;
                }
                logger.info("Received from partition {}, total messages {}" , partition, records.count());
                queue.offer(records);
            }
            stopped = true;
        }
    }
}
