package utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import static java.lang.Math.abs;


public class CompareAvg extends RichSinkFunction<Double> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    public Double avg;

    @Override
    public void open(Configuration parameters) throws Exception{
        this.avg = readAvg();
    }



    @Override
    public void invoke(Double avgStream, Context context) throws Exception{
        if (avg == null) System.out.println("No saved Avg Value");
        if (avgStream != null) {
            if (avgStream == avg) System.out.println("AvgFriendCount: Values Equal, friendscount = " + avg);
            else if (avgStream < avg)
                System.out.println("AvgFriendCount: Stream value smaller, Stream Value = " + avgStream + ", Batch Value = " + avg + ", Difference = " + abs(avg - avgStream));
            else
                System.out.println("AvgFriendCount: Stream value larger, Stream Value = " + avgStream + ", Batch Value = " + avg + ", Difference = " + abs(avg - avgStream));
        }
    }



    private static Double readAvg() {
        String path = "src/main/resources/avg";
        String line = null;
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
                return Double.parseDouble(number);
            }

            // Always close files.
            bufferedReader.close();
        } catch (Exception e) {
            System.out.println(
                    "failed");
        }
        return -1.0;
    }
}
