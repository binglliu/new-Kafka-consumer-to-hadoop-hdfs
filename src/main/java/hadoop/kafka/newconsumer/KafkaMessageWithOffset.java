package hadoop.kafka.newconsumer;

import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KafkaMessageWithOffset implements Writable {
    private Text messageJson;
    private LongWritable offset;

    @Override
    public void write(DataOutput out) throws IOException {
        messageJson.write(out);
        offset.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        messageJson.readFields(in);
        offset.readFields(in);
    }

    public LongWritable getOffset() {
        return offset;
    }

    public Text getMessageJson() {
        return messageJson;
    }

    public KafkaMessageWithOffset() {
        messageJson = new Text();
        offset = new LongWritable(-1l);
    }

    public void setMessageJson(Text messageJson) {
        this.messageJson = messageJson;
    }

    public void setOffset(LongWritable offset) {
        this.offset = offset;
    }
}
