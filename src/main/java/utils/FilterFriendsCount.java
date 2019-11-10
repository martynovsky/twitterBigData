package utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class FilterFriendsCount implements MapFunction<Tuple5<String,String,String,Integer,Integer>, Integer> {
    @Override
    public Integer map(Tuple5<String, String, String, Integer,Integer> tp5) throws Exception {
        return tp5.f4;
    }
}
