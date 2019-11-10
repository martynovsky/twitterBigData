package utils;

import json.PopularityStats;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TupleToJson implements MapFunction<Tuple3<String,String,Integer>, PopularityStats> {
    @Override
    public PopularityStats map(Tuple3<String, String, Integer> tp3) throws Exception {
        return new PopularityStats(tp3.f0,tp3.f1,tp3.f2);
    }
}
