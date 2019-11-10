package utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class FollowerCountToString implements MapFunction<Tuple3<String, String, Integer>, String> {
    @Override
    public String map(Tuple3<String, String, Integer> t) throws Exception {
        return "User with the highest follower count for hashtag " + t.f0 + " is " + t.f1 + " with a follower count of " + t.f2;
    }
}
