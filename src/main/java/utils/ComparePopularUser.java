package utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.util.List;

import static java.lang.Math.abs;


public class ComparePopularUser extends RichSinkFunction<Tuple3<String,String,Integer>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private double[] popularities; //batch value for the popularities
    private String[] popMap = {"huawei","trump","machinelearning","bigdata","deeplearning"};

    @Override
    public void open(Configuration parameters) throws Exception{
        this.popularities = readPopularUsers(); //the order in the array is huawei, trump, machinelearning, bigdata, deeplearning
    }



    @Override
    public void invoke(Tuple3<String,String,Integer> pops, Context context) throws Exception{
        int i = -1;
        if (popularities == null) System.out.println("No saved popularities");
        else {
            //assign i to the position that it has in popMap
            switch (pops.f0){
                case "huawei":
                    i = 0;
                    break;
                case "bigdata":
                    i = 1;
                    break;
                case "machinelearning":
                    i = 2;
                    break;
                case "deeplearning":
                    i = 3;
                    break;
                case "trump":
                    i = 4;
                    break;
            }

            if (pops.f2 == popularities[i]) System.out.println("Most popular user for hashtag " + popMap[i] +  ": Values Equal, popularity = " + popularities[i]);
            else if (pops.f2 < popularities[i])
                System.out.println("Most popular user for hashtag " + popMap[i] + ": Stream value smaller, Stream Value = " + pops.f2 + ", Batch Value = " + popularities[i] + ", Difference = " + abs(popularities[i] - pops.f2));
            else
                System.out.println("Most popular user for hashtag " + popMap[i] + ": Stream value larger, Stream Value = " + pops.f2 + ", Batch Value = " + popularities[i] + ", Difference = " + abs(popularities[i] - pops.f2));
        }
    }



    private static double[] readPopularUsers(){
        String path = "src/main/resources/pop_user";
        String line = null;
        double[] popularities = new double[5];
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(path);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            line = bufferedReader.readLine();
            int i = 0;
            while (line != null) {
                JsonNode node = mapper.readTree(line);
                int n = Integer.parseInt(node.get("n").asText());
                popularities[i] = n;
                line = bufferedReader.readLine();
                i++;
            }
            // Always close files.
            bufferedReader.close();
            return popularities;
        } catch (Exception e) {
            System.out.println(
                    "failed");
        }
        return null;
    }

}
