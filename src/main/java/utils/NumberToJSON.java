package utils;

import json.NumberJSON;
import org.apache.flink.api.common.functions.MapFunction;

public class NumberToJSON implements MapFunction<Double, NumberJSON> {
    @Override
    public NumberJSON map(Double n) throws Exception {
        return new NumberJSON(n);
    }
}
