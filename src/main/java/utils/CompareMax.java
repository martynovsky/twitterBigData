package utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import static java.lang.Math.abs;


public class CompareMax extends RichSinkFunction<Double> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    public Double max;

    @Override
    public void open(Configuration parameters) throws Exception{
        max = readMax();
    }



    @Override
    public void invoke(Double maxStream, Context context) throws Exception{
        if (max == null) System.out.println("No saved Max value");
        if (maxStream != null) {
            if (maxStream == max) System.out.println("MaxFriendCount: Values Equal, Average friendscount = " + max);
            else if (maxStream < max)
                System.out.println("MaxFriendCount: Stream value smaller, Stream Value = " + maxStream + ", Batch Value = " + max + ", Difference = " + abs(max - maxStream));
            else
                System.out.println("MaxFriendCount: Stream value larger, Stream Value = " + maxStream + ", Batch Value = " + max + ", Difference = " + abs(max - maxStream));
        }
    }



    private static Double readMax() {
        String path = "src/main/resources/max";
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
