from pyspark.sql import *
from pyspark import SparkContext
from repository import TwitterRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import sys

spark = SparkSession\
    .builder\
    .appName("Initial TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

#for tweetQuery
try:
    tweetQueryDF = spark.read.json("tweetQuery.json")
    tweetQueryDF.write.parquet("tweetLocation.parquet")
except:
    print(sys.exc_info()[1])

#for tweet
try:
    # tweetDF = spark.read.json("tweet.json")
    tweetsJSON = json.load("tweet.json")
    tweetsJSON = map(lambda tw: tw['created_at'] = tw['created_at']['$date'], tweetsJSON)
    tweetsJSON = map(lambda tw: del tw['_id'], tweetsJSON)
    NewTweets = []
    for tweet in tweetsJSON:
        TwitterRepository.improveTweetCol(tweet)
        normalizedTweet = json_normalize(tweet)
        map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
        NewTweets.append(normalizedTweet.to_json())
    tweetRDD = sc.parallelize(NewTweets)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("write").parquet("tweet.parquet")
except:
    print(sys.exc_info()[1])
