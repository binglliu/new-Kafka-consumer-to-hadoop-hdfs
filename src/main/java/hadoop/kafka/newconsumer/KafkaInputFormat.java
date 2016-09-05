package hadoop.kafka.newconsumer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputFormat extends InputFormat<LongWritable, KafkaMessageWithOffset> {

    static Logger logger = LoggerFactory.getLogger(KafkaInputFormat.class);

    @Override
    public RecordReader<LongWritable, KafkaMessageWithOffset> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new KafkaRecordReader() ;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();

        String topic = conf.get("kafka.topic");
        String group = conf.get("kafka.groupid");
        int partitions = conf.getInt("kafka.topic.partitions", 3);
        String brokers = conf.get("kafka.brokers");
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // split for each partition
        for(int i = 0; i <  partitions; i++) {
            InputSplit split = new KafkaSplit(brokers, topic, partitions, i);
            logger.info("split :" + i + ": " + split);
            splits.add(split);
        }
        return splits;
    }

    public static class KafkaSplit extends InputSplit implements Writable {

        private String brokers;
        private int totalPartitions;
        private int partition;
        private String topic;

        public KafkaSplit() {}

        public KafkaSplit(String brokers, String topic, int totalPartitions, int partition) {
            this.brokers = brokers;
            this.topic = topic;
            this.totalPartitions = totalPartitions;
            this.partition = partition;
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            brokers = Text.readString(in);
            topic = Text.readString(in);
            totalPartitions = in.readInt();
            partition = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokers);
            Text.writeString(out, topic);
            out.writeInt(totalPartitions);
            out.writeInt(partition);
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return Long.MAX_VALUE;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[] {brokers};
        }

        public String getBrokers() {
            return brokers;
        }

        public int getTotalPartitions() {
            return totalPartitions;
        }

        public int getPartition() {
            return partition;
        }

        public String getTopic() {
            return topic;
        }

        @Override
        public String toString() {
            return brokers + ":" + topic + ":" + totalPartitions + ":" + partition;
        }
    }

    public static class KafkaRecordReader extends RecordReader<LongWritable, KafkaMessageWithOffset> {

        private KafkaContext kcontext;
        private KafkaSplit ksplit;
        private TaskAttemptContext context;
        private int limit;
        private LongWritable key;
        private KafkaMessageWithOffset value;
        private long start;
        private long end;
        private long pos;
        private long count = 0L;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.context = context;
            ksplit = (KafkaSplit) split;

            Configuration conf = context.getConfiguration();
            limit = conf.getInt("kafka.limit", -1);


            int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
            int bsize = conf.getInt("kafka.socket.buffersize", 1024*1024);
            int fsize = conf.getInt("kafka.fetch.size", 1024 * 1024);
            String reset = conf.get("kafka.autooffset.reset");
            kcontext = new KafkaContext(ksplit.getBrokers(),
                    ksplit.getTopic(),
                    ksplit.getTotalPartitions(),
                    ksplit.getPartition(),
                    timeout, bsize);

            logger.info("JobId {} {} Start: {} End: {}",
                    new Object[]{context.getJobID(), ksplit, start, end });
        }

        @Override
        public void close() throws IOException {
            kcontext.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public KafkaMessageWithOffset getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {

            if (pos >= end || start == end) {
                return 1.0f;
            }

            if (limit < 0) {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            } else {
                return Math.min(1.0f, count / (float)limit);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new KafkaMessageWithOffset();
            }
            if (limit < 0 || count < limit) {

                long next = kcontext.getNext(key, value);
                if (next >= 0) {
                    pos = next;
                    count++;
                    return true;
                }
            }

            logger.info("Total Messages: " + pos);
            return false;
        }
    }
}

