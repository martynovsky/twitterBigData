package utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

public class FilterFriendsCountToTuple implements MapFunction<Tuple5<String, String, String, Integer,Integer>, Tuple3<String,Integer,Integer>> {

    @Override
    public Tuple3<String,Integer,Integer> map(Tuple5<String,String,String,Integer,Integer> tp) throws Exception {
        return new Tuple3<>(tp.f1,tp.f4,1);
    }

}
