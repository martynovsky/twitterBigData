package json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Tweet implements Serializable {
    private static final long serialVersionUID = 1L;
    Entities entities;
    User user;
    String text;
    private static final ObjectMapper mapper = new ObjectMapper();

    public Tweet(Entities entities, User user, String text){
        this.entities = entities;
        this.user = user;
        this.text = text;
    }

    @Override
    public String toString(){
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "failed to write";
        }
    }
}
