package hhu.bigdata;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.*;

import static hhu.bigdata.TwitterStreamToFile.createSource;
import static utils.DefaultHashtags.createHashtagList;


public class TwitterStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Create a stream containing a tuple in the form of (hashtag,username,text,follower count, friend count)
        //This stream will be the basis of further processing as we will split it up based on this stream
        SingleOutputStreamOperator<Tuple5<String, String, String, Integer, Integer>> streamSource = env.addSource(createSource(createHashtagList()))
                .flatMap(new FilterHashtags(createHashtagList()));


        SingleOutputStreamOperator<Double> avgFriends = streamSource
                //get the name and friend count, put a 1 as the last element of the tuple to sum them
                .map(new FilterFriendsCountToTuple())
                .countWindowAll(3)
                //sum the friend count and the last element which is always a 1
                .reduce(new sumInt())
                //divide sum of friend counts by the sum of tweets
                .map(new FriendsCountAverage());

        DataStream maxFriends = streamSource
                //get the friend count for each tweet
                .map(new FilterFriendsCount())
                .countWindowAll(10)
                //get the maximum friend count in the last 10 tweets
                .reduce(new maxCount())
                .map(new IntegerToDouble());

        DataStream minFriends = streamSource
                //get the friend count for each tweet
                .map(new FilterFriendsCount())
                .countWindowAll(2)
                //get the minimum friend count in the last 10 tweets
                .reduce(new minCount())
                .map(new IntegerToDouble());

        DataStream topFollowerPerHashtag = streamSource
                //get hashtag, username and followercount
                .map(new FilterFollowerCountToTuple())
                //key by the hashtag
                .keyBy(0)
                .countWindow(5)
                //get the user with the highest follower count per hashtag
                .maxBy(2)
                .map(new FollowerCountToString());

        DataStream mostPopularUserPerHashtag = streamSource
                //sum follower and friend count to get popularity
                .map(new CalculatePopularity())
                //key by hashtag
                .keyBy(0)
                .countWindow(3)
                //now return the tweet whose user has the highest popularity
                .maxBy(2);

        //now call compare and predict functions and normal output functions

        maxFriends.addSink(new CompareMax());
        minFriends.addSink(new CompareMin());
        avgFriends.addSink(new CompareAvg());
        mostPopularUserPerHashtag.addSink(new PredictPopularity());
        //avgFriends.print();
        topFollowerPerHashtag.print();
        //mostPopularUserPerHashtag.print();
        env.execute("Twitter Streaming Example");
    }
}