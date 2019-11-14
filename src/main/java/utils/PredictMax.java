package utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import static utils.CompareAvg.readValue;

public class PredictMax extends RichSinkFunction<Double> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private double runningAvg = 0;

    //Initialize Value by taking the average of the batch data
    @Override
    public void open(Configuration parameters) throws Exception {
        this.runningAvg = readValue("max");
    }

    //Build a running average and print the prediction and the actual value to console
    @Override
    public void invoke(Double d, Context context) throws Exception{
        System.out.println("Predicted maximum friend count: " + this.runningAvg + ", Actual maximum friend count: " + d);
        this.runningAvg = 0.9*this.runningAvg + 0.1*d;
    }
}
