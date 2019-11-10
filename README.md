# twitterBigData
Big Data Project using flink based on twitter data.
To run this you need an API Key for twitter. The Analysis is based on five hashtags: bigdata, machinelearning, deeplearning, huawei, trump.

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

The metrics are the same as in Batch Analysis but are perfomed on count windows.
This is implemented in TwitterStream.java.

For the comparison with the batch analysis the metrics Average, Minimum and Maximum friend count are considered.
This is also done in TwitterStream.java. 
If you don't want to compare but just to print out the value 
you can change the function call to match the call for topFollowerPerHashtag.

For the prediction the metric mostPopularUserPerHashtag is used. The prediction is done using
a running Average over all hashtags. The value is initialized on the stored batch analysis.
As with the comparison the call can be changed to just print the actual value of the metric.

The Outputs are console based.
