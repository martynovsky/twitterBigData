package hhu.bigdata;

import json.Tweet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import utils.FilterHashtags;
import utils.HashTagEndPoint;
import utils.TupleToJSON;

import java.util.ArrayList;
import java.util.Properties;

import static utils.DefaultHashtags.createHashtagList;


public class TwitterStreamToFile {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Create a stream containing a tuple in the form of (hashtag,username,text,follower count, friend count)
        //convert this to a json object for writing to file
        SingleOutputStreamOperator<Tweet> streamSource = env.addSource(createSource(createHashtagList()))
                .flatMap(new FilterHashtags(createHashtagList()))
                .map(new TupleToJSON());

        streamSource.writeAsText("src/main/resources/test_data1", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //streamSource.writeAsText("/home/yulian/Downloads/blockkurs/reddit/src/main/resources/test_data");
        env.execute("Twitter Streaming Example");
    }


    public static TwitterSource createSource(ArrayList hashtags){
        Properties props = new Properties();
        //set twitter keys here


        HashTagEndPoint customInitializer = new HashTagEndPoint(hashtags);
        TwitterSource source = new TwitterSource(props);
        source.setCustomEndpointInitializer(customInitializer);
        return source;
    }

}