package utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import static java.lang.Math.abs;


public class CompareTopFollower extends RichSinkFunction<Tuple3<String,String,Integer>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private double[] followers; //batch value for the popularities
    private String[] indexMap = {"trump","bigdata","deeplearning","machinelearning","huawei"};

    @Override
    public void open(Configuration parameters) throws Exception{
        this.followers = readTopFollower(); //the order in the array is trump,bigdata,deeplearning,machinelearning,huawei
    }



    @Override
    public void invoke(Tuple3<String,String,Integer> follower, Context context) throws Exception{
        int i = -1;
        if (followers == null) System.out.println("No saved top follower");
        else {
            //assign i to the position that it has in popMap
            switch (follower.f0){
                case "trump":
                    i = 0;
                    break;
                case "bigdata":
                    i = 1;
                    break;
                case "deeplearning":
                    i = 2;
                    break;
                case "machinelearning":
                    i = 3;
                    break;
                case "huawei":
                    i = 4;
                    break;
            }

            if (follower.f2 == followers[i]) System.out.println("Top follower count for hashtag " + indexMap[i] +  ": Values Equal, popularity = " + followers[i]);
            else if (follower.f2 < followers[i])
                System.out.println("Top follower count for hashtag " + indexMap[i] + ": Stream value smaller, Stream Value = " + follower.f2 + ", Batch Value = " + followers[i] + ", Difference = " + abs(followers[i] - follower.f2));
            else
                System.out.println("Top follower count for hashtag " + indexMap[i] + ": Stream value larger, Stream Value = " + follower.f2 + ", Batch Value = " + followers[i] + ", Difference = " + abs(followers[i] - follower.f2));
        }
    }



    private static double[] readTopFollower(){
        String path = "src/main/resources/top_follower";
        String line = null;
        double[] followers = new double[5];
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
                followers[i] = n;
                line = bufferedReader.readLine();
                i++;
            }
            // Always close files.
            bufferedReader.close();
            return followers;
        } catch (Exception e) {
            System.out.println(
                    "failed");
        }
        return null;
    }

}
