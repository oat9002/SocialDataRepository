from pyspark import SparkContext
from pyspark.sql import *

import sys

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

def saveTweet(tweets):
    tweetParquet = "tweet.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    tweetBaseDF.createOrReplaceTempView("tweet")
    tweetArr = []
    for tweet in tweets.status:
        existTweet = spark.sql("SELECT id FROM tweet WHERE id_str == '%s'" % tweet['id_str'])
        if existTweet.count() == 0:
            tweetArr.append(tweet)
    tweetRDD = sc.parallelize(tweetArr)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("append").parquet(tweetParquet)

def saveTweetQuery(tweetQuery):
    tweetQueriesParquet = "tweetQuery.parquet"
    tweetQueryBaseDF = spark.read.parquet(tweetQueriesParquet)
    tweetQueryBaseDF.createOrReplaceTempView("tweetQuery")
    existTweetQuery = spark.sql("SELECT query FROM tweetQuery WHERE query == '%s' " % tweetQuery['query'])
    if existTweetQuery.count() == 0:
        tweetQueryRDD = sc.parallelize([tweetQuery])
        tweetQueryDF = spark.read.json(tweetQueryRDD)
        tweetQueryDF.write.mode("append").parquet(tweetQueriesParquet)
