package utils;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.ArrayList;

public class HashTagEndPoint implements TwitterSource.EndpointInitializer, Serializable {

    private ArrayList<String> hashtags;

    public HashTagEndPoint(ArrayList<String> hashtags) {
        this.hashtags=hashtags;
    }

    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(this.hashtags);
        return endpoint;
    }
}