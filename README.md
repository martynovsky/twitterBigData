# twitterBigData
Big Data Project using flink based on twitter data.
To run this you need an API Key for twitter. The Analysis is based on five hashtags: bigdata, machinelearning, deeplearning, huawei, trump.

# Setup
Clone the project with git clone https://github.com/martynovsky/twitterBigData.git
Import the project to InteliJ and let it install then dependencies.
Then you can use it as described in the next sections by running the associated main() function.



# Data Collection
Data Collection is implemented in TwitterStreamToFile.java.
Data is stored in a file in Json format.

# Batch Analysis
The following five different metrics are implemented:
* Average friend count
* Maximum friend count
* Minimum friend count
* User with the most followers per hashtag
* User with the highest popularity per hashtag

The analysis is implemented in TwitterBatch.java.

# Stream Analysis

The metrics are the same as in Batch Analysis but are perfomed on count windows (for the purpose of demonstration these are set to low values).
This is implemented in TwitterStream.java.

For the comparison with the batch analysis all 5 metrics are considered.
This is also done in TwitterStream.java. 

For the prediction the metric mostPopularUserPerHashtag is used. The prediction is done using
a running Average over all hashtags. The value is initialized on the stored batch analysis.

If you only want comparisons or predictions you can comment the associated function calls.
If you only want to see the output of the streams without comparison or prediction you can replace the comparison and predicion calls with ".print()".

The Outputs are console based.
