package utils;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class SumInt implements ReduceFunction<Tuple3<String, Integer, Integer>> {
    @Override
    public Tuple3<String,Integer,Integer> reduce(Tuple3<String,Integer,Integer> a, Tuple3<String,Integer,Integer> b) throws Exception {
        return new Tuple3<String, Integer, Integer>("",a.f1 + b.f1, a.f2 + b.f2);
    }
}
