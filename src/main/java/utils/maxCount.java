package utils;

import org.apache.flink.api.common.functions.ReduceFunction;

import static java.lang.Math.max;

public class maxCount implements ReduceFunction<Integer> {

    @Override
    public Integer reduce(Integer integer, Integer t1) throws Exception {
        return max(integer,t1);
    }
}
