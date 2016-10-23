from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

def saveTweet(tweets):
    tweetParquet = "tweet.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    # tweetBaseDF.createOrReplaceTempView("tweet")
    tweetArr = []
    for tweet in tweets['tweets']:
        # existTweet = spark.sql("SELECT id_str FROM tweet WHERE id_str == '%s'" % tweet['id_str'])
        existTweet = tweetBaseDF.where(tweetBaseDF.id_str['0'] == tweet['id_str'])
        if existTweet.count() == 0:
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
    print tweet.select(tweet.text).collect()

def saveTweetQuery(tweetQuery):
    tweetQueriesParquet = "tweetQuery.parquet"
    tweetQueryBaseDF = spark.read.parquet(tweetQueriesParquet)
    tweetQueryBaseDF.createOrReplaceTempView("tweetQuery")
    existTweetQuery = spark.sql("SELECT query FROM tweetQuery WHERE query == '%s' " % tweetQuery['query'])
    if existTweetQuery.count() == 0:
        tweetQueryRDD = sc.parallelize([tweetQuery])
        tweetQueryDF = spark.read.json(tweetQueryRDD)
        tweetQueryDF.show()
        # tweetQueryDF.write.mode("append").parquet(tweetQueriesParquet)
