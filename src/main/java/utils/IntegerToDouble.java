package utils;

import org.apache.flink.api.common.functions.MapFunction;

public class IntegerToDouble implements MapFunction<Integer, Double> {
    @Override
    public Double map(Integer integer) throws Exception {
        return 1.0*integer;
    }
}
