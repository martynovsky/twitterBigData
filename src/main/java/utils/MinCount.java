package utils;

import org.apache.flink.api.common.functions.ReduceFunction;

import static java.lang.Math.min;

public class MinCount implements ReduceFunction<Integer> {

    @Override
    public Integer reduce(Integer integer, Integer t1) throws Exception {
        return min(integer, t1);
    }
}