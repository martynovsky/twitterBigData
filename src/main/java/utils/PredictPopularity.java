package utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class PredictPopularity extends RichSinkFunction<Tuple3<String,String,Integer>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private double runningAvg = 0;

    //Initialize Value by taking the average of the batch data
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("blub");
        this.runningAvg = readPopularityAvg();
    }

    //Build a running average and print the prediction and the actual value to console
    @Override
    public void invoke(Tuple3<String,String,Integer> t, Context context) throws Exception{
        System.out.println("Predicted Popularity for most popular user : " + this.runningAvg + ", Actual highest Popularity: " + t.f2);
        this.runningAvg = 0.9*this.runningAvg + 0.1*t.f2;
    }

    private static double readPopularityAvg() {
        String path = "src/main/resources/pop_user";
        String line = null;
        double avg = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(path);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            line = bufferedReader.readLine();
            if (line != null) {
                String number = mapper.readTree(line).get("n").asText();
                avg += Double.parseDouble(number);
            }
            //divide by 5 as we look at 5 hashtags and this will be an average over all hashtags
            avg = avg/5;

            // Always close files.
            bufferedReader.close();
            return avg;
        } catch (Exception e) {
            System.out.println(
                    "failed");
        }
        return -1.0;
    }

}