package json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)

public class NumberJSON implements Serializable {
    private static final long serialVersionUID = 1L;
    Double n;
    private static final ObjectMapper mapper = new ObjectMapper();


    public NumberJSON(Double n){
        this.n = n;
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
