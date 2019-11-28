package hhu.bigdata;

import json.NumberJSON;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import utils.*;

import static utils.DefaultHashtags.createHashtagList;

public class TwitterBatch {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //read in data from stored file
        //further calculations will be done based on this stream
        DataSet<Tuple5<String, String, String, Integer, Integer>> batchSource = env.readTextFile("src/main/resources/test_data")
                //convert to tuple
                .flatMap(new FilterHashtags(createHashtagList()));


        DataSet avgFriends = batchSource
                //get the name and friend count, put a 1 as the last element of the tuple to sum them
                .map(new FilterFriendsCountToTuple())
                //group by name and take the first one in each group to make users unique
                .groupBy(0)
                .first(1)
                //sum friend count and the last entry which is always a 1
                .reduce(new SumInt())
                //divide friend count sum by sum of users
                .map(new FriendsCountAverage())
                //wrap in an object for writing to file
                .map(new NumberToJSON());

        DataSet<NumberJSON> maxFriends = batchSource
                //get friend count for each tweet
                .map(new FilterFriendsCount())
                //find maximum friend count
                .reduce(new MaxCount())
                //cast to double
                .map(new IntegerToDouble())
                //wrap in an object for writing to file
                .map(new NumberToJSON());

        DataSet<NumberJSON> minFriends = batchSource
                //get friend count for each tweet
                .map(new FilterFriendsCount())
                //find minimum friend count
                .reduce(new MinCount())
                //cast to double
                .map(new IntegerToDouble())
                //wrap in an object for writing to file
                .map(new NumberToJSON());

        DataSet topFollowerPerHashtag = batchSource
                //get hashtag, username and follower count
                .map(new FilterFollowerCountToTuple())
                //group by hashtag
                .groupBy(0)
                //get tweet with maximum follower count
                .maxBy(2)
                //transform to json object for writing to file
                .map(new TupleToJson());

        //Popularity = FollowerCount + FriendCount
        DataSet mostPopularUserPerHashtag = batchSource
                //sum follower and friend count to get popularity
                .map(new CalculatePopularity())
                //group by hashtag
                .groupBy(0)
                //get tweet with maximum popularity
                .maxBy(2)
                //transform to json object for writing to file
                .map(new TupleToJson());

        mostPopularUserPerHashtag.writeAsText("src/main/resources/pop_user", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        topFollowerPerHashtag.writeAsText("src/main/resources/top_follower", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        minFriends.writeAsText("src/main/resources/min", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        maxFriends.writeAsText("src/main/resources/max", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        avgFriends.writeAsText("src/main/resources/avg", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("Random Name");
    }
}