import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.util.Random;

/** */
public class MyEvent {
    public MyEvent() {}

    public MyEvent(long valueIn) {
        value = valueIn;
        id = new Random().nextLong();
        timestamp = new Timestamp(System.currentTimeMillis());
    }

    public MyEvent(long id, long valueIn, Timestamp timestampIn) {
        id = id;
        value = valueIn;
        timestamp = timestampIn;
    }

    public Timestamp timestamp;

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public long value;

    public long getValue() {
        return value;
    }

    public void setValue(long valueIn) {
        value = valueIn;
    }

    public long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        id = id;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static MyEvent getSampleEvent() {
        long initMillis = System.currentTimeMillis();
        return new MyEvent(
                new Random().nextLong(), new Random().nextInt(5), new Timestamp(initMillis));
    }
}
