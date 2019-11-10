package json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class User implements Serializable {
    String name;
    Integer followers_count,friends_count;

    public User(String name, Integer followers_count, Integer friends_count){
        this.name = name;
        this.followers_count = followers_count;
        this.friends_count = friends_count;
    }
}
