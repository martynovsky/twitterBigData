package utils;

import json.Entities;
import json.Hashtag;
import json.Tweet;
import json.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class TupleToJSON implements MapFunction<Tuple5<String,String,String,Integer,Integer>, Tweet> {

    @Override
    public Tweet map(Tuple5<String, String, String, Integer,Integer> t5) throws Exception {
        Hashtag hashtag = new Hashtag(t5.f0);
        Entities entities = new Entities(hashtag);
        User user = new User(t5.f1,t5.f3,t5.f4);
        Tweet tweet = new Tweet(entities, user, t5.f2);
        return tweet;
    }
}