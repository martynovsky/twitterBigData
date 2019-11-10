package json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Hashtag implements Serializable {
    String text;

    public Hashtag(String text){
        this.text = text;
    }
}
