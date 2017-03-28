# -*- coding: utf-8 -*-
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
from repository import TwitterRepository
from repository import SocialDataRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import os.path as path
import pydoop.hdfs

spark = SparkSession\
    .builder\
    .master("spark://stack-02:7077")\
    .config("spark.cores.max", 2)\
    .appName("Initial TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext
hdfs = pydoop.hdfs.hdfs()

# for place
# if not hdfs.exists("PLACE.parquet"):
#     with open("place.json", "r") as file:
#         placeJson = json.load(file)
#         for place in placeJson['places']:
#             SocialDataRepository.savePlace(SocialDataRepository.createPlaceSchema(place))

# for query
# if not hdfs.exists("hdfs://stack-02:9000/SocialDataRepository/QUERY.parquet"):
#     with open('tweetQuery.json') as json_data:
#         queryJson = json.load(json_data)
#         for query in queryJson['queries']:
#             SocialDataRepository.addPlaceOrQuery(query)

# for tweet
if not hdfs.exists("hdfs://stack-02:9000/SocialDataRepository/TW_TWEET.parquet"):
    with open('tweet.json') as json_data:
        tweetsJSON = json.load(json_data)
        for index in range(0, len(tweetsJSON)):
            tweetsJSON[index]['created_at'] = tweetsJSON[index]['created_at']['$date']
            if("retweeted_status" in tweetsJSON[index]):
                if tweetsJSON[index]['retweeted_status'] != None:
                    tweetsJSON[index]['text'] = tweetsJSON[index]['retweeted_status']['text']
            del tweetsJSON[index]['_id']

        tweets = []
        tweet_backup = []
        users = []
        places = []
        queryDF = spark.read.parquet("hdfs://stack-02:9000/SocialDataRepository/QUERY.parquet")
        keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
        print(queryDF.count())
        for tweet in tweetsJSON:
            for keyword in keywords:
                if keyword['keyword'] in tweet['text']:
                    tweets.append(TwitterRepository.selectTweetCol(tweet, keyword['id']))
                    tweet_backup.append(tweet)
            users.append(TwitterRepository.selectUserCol(tweet['user']))
        print(len(tweets))
        tweetRDD = sc.parallelize(tweets)
        tweetDF = spark.createDataFrame(tweetRDD)
        tweetDF.printSchema()
        print("total parquets: ", tweetDF.count())
        tweetDF.write.parquet("hdfs://stack-02:9000/SocialDataRepository/TW_TWEET.parquet")
        userRDD = sc.parallelize(users)
        userDF = spark.createDataFrame(userRDD)
        if not hdfs.exists("hdfs://stack-02:9000/SocialDataRepository/TW_USER.parquet"):
            userDF.write.parquet('hdfs://stack-02:9000/SocialDataRepository/TW_USER.parquet')
            userDF.printSchema()
        TwitterRepository.saveRawTweet(tweet_backup)
        with open("TW_TWEET_BACKUP.json", 'r') as test:
            json = [json.loads(line) for line in test]
            print("total backup: ", len(json))


# spare code
#     rawTweets = []
#     for tweet in tweetsJSON:
#         normalizedTweet = json_normalize(tweet)
#         map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
#         rawTweets.append(normalizedTweet.to_json())
#     rawTweetRDD = sc.parallelize(rawTweets)
#     rawTweetDF = spark.read.json(rawTweetRDD)
#     rawTweetDF.write.parquet("rawTweet.parquet")
