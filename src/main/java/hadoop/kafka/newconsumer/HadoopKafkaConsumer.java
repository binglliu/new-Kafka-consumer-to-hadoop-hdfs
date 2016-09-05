package hadoop.kafka.newconsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class HadoopKafkaConsumer extends Configured implements Tool {
    private final static Logger logger = LoggerFactory.getLogger(HadoopKafkaConsumer.class);

    public static class KafkaMapper extends Mapper<LongWritable, KafkaMessageWithOffset, LongWritable, KafkaMessageWithOffset> {
        @Override
        public void map(LongWritable key, KafkaMessageWithOffset value, Context context) throws IOException {
            try {
                context.write(key, value);
            } catch (InterruptedException e) {
                logger.error("KafkaMapper map context write failed: ", e);
            }
        }
    }

    public static class KafkaReducer extends Reducer<LongWritable, KafkaMessageWithOffset, NullWritable, Text> {
        Gson gson = new Gson();

        @Override
        public void reduce(LongWritable key, Iterable<KafkaMessageWithOffset> values, Context context)
                throws IOException, InterruptedException {

            // map to save unique message from kafka
            Map<Long, Pair<MyKafkaMessage, Long>> messages = new HashMap<>();
            for (KafkaMessageWithOffset message : values) {
                // deserialize to pojo, dedup then json output
                if(message == null) {
                    logger.error("Invalid message");
                    continue;
                }
                try {
                    String json = message.getMessageJson().toString();
                    MyKafkaMessage obj = gson.fromJson(json, MyKafkaMessage.class);
                    long offset = message.getOffset().get();

                    if (obj != null) {
                        Long id = obj.getId();
                        if(messages.containsKey(id)) {
                            Pair<MyKafkaMessage, Long> oldMessage = messages.get(id);
                            if(offset > oldMessage.getRight()) {
                                //overwrite with newer message
                                messages.put(id, new MutablePair<>(obj, offset));
                                logger.info("Update newer message {}, offset {}", id, offset);
                            } else {
                                //drop this message because it is older than the one in the map
                                logger.info("Drop older message {}, offset {}", id, offset);
                            }
                        } else {
                            // first time message
                            messages.put(id, new MutablePair<>(obj, offset));
                            logger.info("Add message {}, offset {}", id, offset);
                        }
                    } else {
                        logger.error("message is null");
                    }
                } catch (Exception e) {
                    logger.error("Invalid kafka message, exception: ", e);
                }
            }

            for (Pair<MyKafkaMessage, Long> objPair : messages.values()) {
                if (objPair != null && objPair.getLeft() != null) {
                    MyKafkaMessage msg = objPair.getLeft();
                    if(msg != null) {
                        String output = gson.toJson(msg);
                        context.write(null, new Text(output));
                        logger.info("Write message {} to HDFS", output);
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = buildOptions();

        Configuration conf = getConf();
        GenericOptionsParser parser = new GenericOptionsParser(conf, options, args);
        CommandLine cmd = parser.getCommandLine();
        conf.set("kafka.topic", cmd.getOptionValue("topic", "my_test_kafka_topic"));
        conf.set("kafka.groupid", cmd.getOptionValue("consumer-group", "my_test_kafka_group_id"));
        conf.set("kafka.brokers", cmd.getOptionValue("brokers", "localhost:9092"));
        conf.setInt("kafka.limit", Integer.valueOf(cmd.getOptionValue("limit", "-1")));
        conf.setInt("kafka.topic.partitions", Integer.valueOf(cmd.getOptionValue("partitions", "3")));
        conf.set("hdfs.output.dir", cmd.getOptionValue("output", "my_test_output_dir"));

        conf.setBoolean("mapred.map.tasks.speculative.execution", false);

        logger.info("configuration -- topic: " + conf.get("kafka.topic") +
                ", brokers: " + conf.get("kafka.brokers") +
                ", groupId: " + conf.get("kafka.groupid") +
                ", partition: " + conf.get("kafka.topic.partitions"));

        Job job = new Job(conf, "my hadoop kafka consumer");
        job.setJarByClass(getClass());
        job.setMapperClass(KafkaMapper.class);
        job.setReducerClass(KafkaReducer.class);

        // input
        job.setInputFormatClass(KafkaInputFormat.class);

        //output
        Path outputPath = new Path(conf.get("hdfs.output.dir"));
        logger.info("hdfs output path: " + outputPath);

        // clean up output directory first
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            logger.warn(String.format("Deleting pre-existing Path " + outputPath));
            if(fs.delete(outputPath, true)) {
                logger.info("Deleted.");
            } else {
                logger.error("Failed to delete.");
            }
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(KafkaMessageWithOffset.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0: -1;
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("output")
                .withLongOpt("output")
                .hasArg()
                .withDescription("output directory")
                .create("o"));

        options.addOption(OptionBuilder.withArgName("topic")
                .withLongOpt("topic")
                .hasArg()
                .withDescription("kafka topic")
                .create("t"));

        options.addOption(OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("kafka consumer groupid")
                .create("g"));

        options.addOption(OptionBuilder.withArgName("brokers")
                .withLongOpt("brokers")
                .hasArg()
                .withDescription("ZooKeeper connection String")
                .create("b"));

        options.addOption(OptionBuilder.withArgName("partitions")
                .withLongOpt("partitions")
                .hasArg()
                .withDescription("kafka topic partitions")
                .create("p"));

        return options;
    }

    public static void main(String[] args) throws Exception {
        logger.info("Start HadoopKafkaConsumer.");

        int exitCode = ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new HadoopKafkaConsumer(), args);
        System.exit(exitCode);
    }
}
