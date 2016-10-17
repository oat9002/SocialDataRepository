from pyspark import SparkContext
from pyspark.sql import *

import sys

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

# def saveTweet(tweets):
#     tweetParquet = "tweet.parquet"
#     tweetDF = spark.read.parquet(tweetParquet)
#     tweetDf.createOrReplaceTempView("tweet")
#     for tweet in tweets.status:
#         existTweet = spark.sql("SELECT id FROM tweet WHERE id == tweet.id")
#         if existTweet is None:
#             tweetDF.write.mode("append").parquet(tweetParquet)

def saveTweetQuery(tweetQuery):
    tweetQueriesParquet = "tweetQuery.parquet"
    tweetQueryBaseDF = spark.read.parquet(tweetQueriesParquet)
    tweetQueryBaseDF.createOrReplaceTempView("tweetQuery")
    existTweetQuery = spark.sql("SELECT query FROM tweetQuery WHERE query == tweetQuery['query']")
    if existTweetQuery is None:
        tweetQuery['_id': "123123"]
        tweetQueryRDD = sc.parallelize(tweetQuery)
        tweetQueryDF = spark.read.json(tweetQueryRDD)
        tweetQueryBaseDF.write.mode("append").parquet(tweetQueriesParquet)
