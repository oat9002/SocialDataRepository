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

spark = SparkSession\
    .builder\
    .appName("Initial TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext


if not path.exists("PLACE.parquet"):
    with open("place.json", "r") as file:
        placeJson = json.load(file)
        places = []
        for place in placeJson['places']:
            places.append(SocialDataRepository.createPlaceSchema(place))
        placeRDD = sc.parallelize(places)
        placeDF = spark.createDataFrame(placeRDD)
        placeDF.write.parquet("PLACE.parquet")
        placeDF.printSchema()

#for query
if not path.exists("QUERY.parquet"):
    with open('tweetQuery.json') as json_data:
        queryJson = json.load(json_data)
        queries = []
        placeDF = spark.read.parquet("PLACE.parquet")
        places = placeDF.select(placeDF.id, placeDF.name, placeDF.geolocation).collect()
        for query in queryJson['queries']:
            samePlace = False
            placeGoogle = SocialDataRepository.getPlacesFromGoogle(query['keyword'])
            for place in places:
                samePlace = SocialDataRepository.compareQueryAndPlace(place, placeGoogle, query['keyword'])
                if samePlace:
                   break
            if samePlace:
                queries.append(SocialDataRepository.createQuerySchema(query, place['id']))
            else:
                queries.append(SocialDataRepository.createQuerySchema(query, 0))
        queryRDD = sc.parallelize(queries)
        queryDF = spark.createDataFrame(queryRDD)
        queryDF.write.parquet("QUERY.parquet")
        queryDF.printSchema()

# for tweet
if not path.exists("TW_TWEET.parquet"):
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
        queryDF = spark.read.parquet("QUERY.parquet")
        keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
        for tweet in tweetsJSON:
            for keyword in keywords:
                if keyword['keyword'] in tweet['text']:
                    tweets.append(TwitterRepository.selectTweetCol(tweet, keyword['id']))
                    tweet_backup.append(tweet)
            users.append(TwitterRepository.selectUserCol(tweet['user']))
        tweetRDD = sc.parallelize(tweets)
        tweetDF = spark.createDataFrame(tweetRDD)
        tweetDF.printSchema()
        print("total parquets: ", tweetDF.count())
        tweetDF.write.parquet("TW_TWEET.parquet")
        userRDD = sc.parallelize(users)
        userDF = spark.createDataFrame(userRDD)
        if not path.exists("TW_USER.parquet"):
            userDF.write.parquet('TW_USER.parquet')
            userDF.printSchema()
        TwitterRepository.saveRawTweet(tweet_backup)
        with open("TW_TWEET_BACKUP.json", 'r') as test:
            json = [json.loads(line) for line in test]
            print("total backup: ", len(json))


#spare code
    # rawTweets = []
    # for tweet in tweetsJSON:
    #     normalizedTweet = json_normalize(tweet)
    #     map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
    #     rawTweets.append(normalizedTweet.to_json())
    # rawTweetRDD = sc.parallelize(rawTweets)
    # rawTweetDF = spark.read.json(rawTweetRDD)
    # rawTweetDF.write.parquet("rawTweet.parquet")

        