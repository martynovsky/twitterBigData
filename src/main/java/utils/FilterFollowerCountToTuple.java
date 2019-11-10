package utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

public class FilterFollowerCountToTuple implements MapFunction<Tuple5<String,String,String,Integer,Integer>, Tuple3<String,String,Integer>> {

    @Override
    public Tuple3<String,String, Integer> map(Tuple5<String, String, String, Integer,Integer> tp5) throws Exception {
        return new Tuple3<String,String,Integer>(tp5.f0 ,tp5.f1,tp5.f3);
    }
}
