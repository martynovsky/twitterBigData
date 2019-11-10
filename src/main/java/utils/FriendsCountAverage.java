package utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class FriendsCountAverage implements MapFunction<Tuple3<String,Integer,Integer>, Double> {
    @Override
    public Double map(Tuple3<String,Integer,Integer> tp) throws Exception {
        return 1.0*tp.f1/tp.f2;
    }
}
