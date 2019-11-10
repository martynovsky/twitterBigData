package utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/*
1. filter by hashtag
2. user->name
3. text
4. follower count
5. friend count
 */
public class FilterHashtags implements FlatMapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private ArrayList<String> hashtag;

    public FilterHashtags(ArrayList<String> hashtag){
        if (hashtag == null) {
            this.hashtag = new ArrayList<String>();
            this.hashtag.add("usa");
            this.hashtag.add("president");
            this.hashtag.add("brexit");
            this.hashtag.add("gamer");
        }
        else {
            this.hashtag = hashtag;
        }
    }

    @Override
    public void flatMap(String s, Collector<Tuple5<String,String,String,Integer,Integer>> collector) throws Exception {

        JsonNode tweetJson = mapper.readTree(s);
        JsonNode delete = tweetJson.get("delete");
        if (delete != null) return;
        JsonNode userNode = tweetJson.get("user");
        if (userNode == null) {
            return;
        }
        //System.out.println(userNode.asText());

        JsonNode entitiesNode = tweetJson.get("entities");
        if (entitiesNode == null) {
            return;
        }
        JsonNode hashtagNode = entitiesNode.get("hashtags");

        if (hashtagNode == null) return;


        for (Iterator<JsonNode> iter = hashtagNode.elements(); iter.hasNext();) {
            JsonNode node = iter.next();
            String hashtag = node.get("text").asText();
            if (this.hashtag.contains(hashtag)) {
                String text = tweetJson.get("text").asText();

                String name = userNode.get("name").asText();
                Integer followersCount = Integer.parseInt(userNode.get("followers_count").asText());
                Integer friendCount = Integer.parseInt(userNode.get("friends_count").asText());
                collector.collect(new Tuple5<String, String, String, Integer, Integer>(hashtag,name,text,followersCount,friendCount));
                /*
                User user = new User(name, location, followerCount, friendCount);
                Entities entities = new Entities(hashtag);
                Tweet tweet = new Tweet(entities, user, text);
                collector.collect(tweet);*/
            }
        }
    }

}
