from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
import dateutil.parser as date

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

def saveTweet(tweets):
    tweetParquet = "tweetTest.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    tweetArr = []
    for tweet in tweets['tweets']:
        existTweet = tweetBaseDF.where(tweetBaseDF.id_str['0'] == tweet['id_str'])
        if existTweet.count() == 0:
            improveTweetCol(tweet)
            normalizedTweet = json_normalize(tweet)
            map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
            tweetArr.append(normalizedTweet.to_json())
    tweetRDD = sc.parallelize(tweetArr)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("append").parquet(tweetParquet)

def readTweet():
    tweetParquet = "tweetTest.parquet"
    tweetDF = spark.read.parquet(tweetParquet)
    tweetDF.createOrReplaceTempView("tweet")
    tweet = tweetDF.where(tweetDF.id_str['0'] == "790280483972194305")
    tweet.show()
    print tweet.select(tweet.text).collect()

def improveTweetCol(tweet):
    del tweet['user'] #remove user column
    del tweet['retweeted_status'] #remove retweet_status column
    tweet['created_at'] = date.parse(tweet['created_at']) #change to i

# def saveTweetQuery(tweetQuery):
#     tweetQueriesParquet = "tweetQuery.parquet"
#     tweetQueryBaseDF = spark.read.parquet(tweetQueriesParquet)
#     existTweetQuery = tweetQueryBaseDF.where(tweetQueryBaseDF.)
#     if existTweetQuery.count() == 0:
#         tweetQueryRDD = sc.parallelize([tweetQuery])
#         tweetQueryDF = spark.read.json(tweetQueryRDD)
#         tweetQueryDF.show()
