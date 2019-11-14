package utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import static utils.CompareAvg.readValue;

public class PredictAvg extends RichSinkFunction<Double> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private double runningAvg;

    //Initialize Value by taking the average of the batch data
    @Override
    public void open(Configuration parameters) throws Exception {
        this.runningAvg = readValue("avg");
    }

    @Override
    public void invoke(Double d, Context context) throws Exception{
        System.out.println("Predicted avg friend count: " + this.runningAvg + ", Actual avg friend count: " + d);
        this.runningAvg = 0.9*this.runningAvg + 0.1*d;
    }
}
