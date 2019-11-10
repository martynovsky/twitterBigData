package json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;
import java.util.ArrayList;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Entities implements Serializable {
    ArrayList<Hashtag> hashtags;

    public Entities(Hashtag hashtags){
        this.hashtags = new ArrayList<>();
        this.hashtags.add(hashtags);
    }
}
