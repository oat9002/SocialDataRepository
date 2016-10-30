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
    rawTweets = []
    for tweet in tweetsJSON:
        normalizedTweet = json_normalize(tweet)
        map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
        rawTweets.append(normalizedTweet.to_json())
    rawTweetRDD = sc.parallelize(rawTweets)
    rawTweetDF = spark.read.json(rawTweetRDD)
    rawTweetDF.write.parquet("rawTweet.parquet")

    tweets = []
    users = []
    places = []
    for tweet in tweetsJSON:
        tweets.append(TwitterRepository.selectTweetCol(tweet))
        users.append(TwitterRepository.selectUserCol(tweet['user']))
        places.append(TwitterRepository.selectPlaceCol(tweet['place'])) if tweet['place'] != "null"
    tweetRDD = sc.parallelize(tweets)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.parquet("tweet.parquet")
    userRDD = sc.parallelize(users)
    userDF = spark.read.json(userRDD)
    userDF.write.parquet('tweetUser.parquet')
    placeRDD = sc.parallelize(places)
    placeDF = sc.parallelize(placeRDD)
    placeDF.write.parquet("tweetPlace.parquet")

except:
    print(sys.exc_info()[1])
