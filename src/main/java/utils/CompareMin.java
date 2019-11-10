package utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import static java.lang.Math.abs;


public class CompareMin extends RichSinkFunction<Double> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    public Double min;

    @Override
    public void open(Configuration parameters) throws Exception{
        min = readMin();
    }



    @Override
    public void invoke(Double minStream, Context context) throws Exception{
        if (min == null) System.out.println("No saved Min value");
        if (minStream != null) {
            if (minStream == min) System.out.println("MinFriendCount: Values Equal, Average friendscount = " + min);
            else if (minStream < min)
                System.out.println("MinFriendCount: Stream value smaller, Stream Value = " + minStream + ", Batch Value = " + min + ", Difference = " + abs(min - minStream));
            else
                System.out.println("MinFriendCount: Stream value larger, Stream Value = " + minStream + ", Batch Value = " + min + ", Difference = " + abs(min - minStream));
        }
    }



    private static Double readMin() {
        String path = "src/main/resources/min";
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
