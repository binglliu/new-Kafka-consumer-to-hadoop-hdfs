package hadoop.kafka.newconsumer;

import com.google.gson.Gson;

public class MyKafkaMessage {
    private Long id;
    private String msg;

    public MyKafkaMessage() {
        id = 0l;
        msg = "";
    }

    public MyKafkaMessage(Long id, String msg) {
        this.id = id;
        this.msg = msg;
    }

    public Long getId() {
        return id;
    }

    public String getMsg() {
        return msg;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
