# -*- coding: utf-8 -*-
import sys
import os
sys.path.append('../service')
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from service.SocialDataService import writeParquet
from geopy.distance import great_circle
import dateutil.parser as date
import uuid
import json


spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

#TW_TWEET table
def saveTweet(tweets, queryId): #queryId is packed with Tweet Data
    tweetParquet = "TW_TWEET.parquet"
    userParquet = "TW_USER.parquet"
    tweetArr = []
    if os.path.exists(tweetParquet):
        tweetBaseDF = spark.read.parquet(tweetParquet)
        tweetArrBackup = []
        for tweet in tweets:
            existTweet = tweetBaseDF.where(tweetBaseDF.id == tweet['id_str'])
            if existTweet.count() == 0:
                tweetArr.append(selectTweetCol(tweet, queryId))
                saveUserFromTweet(tweet['user'])
                tweetArrBackup.append(tweet)
        saveRawTweet(tweetArrBackup)
    else:
        for tweet in tweets:
            tweetArr.append(selectTweetCol(tweet, queryId))
            saveUserFromTweet(tweet['user'])
        saveRawTweet(tweets)
    print len(tweetArr)
    writeParquet(tweetParquet, tweetArr, sc, spark)
    return tweetArr #for saving to Social table


def selectTweetCol(tweet, queryId):
    newTweet = {}
    newTweet['id'] = tweet['id_str']
    newTweet['created_at'] = date.parse(tweet['created_at']).isoformat()
    newTweet['text'] = tweet['text']
    hashtags = []
    if tweet['entities']['hashtags'] != None:
        for ht in tweet['entities']['hashtags']:
            hashtags.append(ht['text'])
    newTweet['hashtags'] = hashtags
    newTweet['geolocation'] = tweet['coordinates']['coordinates'] if tweet['coordinates'] != None else []
    newTweet['favorite_count'] = tweet['favorite_count'] if tweet['favorited'] != False or  tweet['favorited'] != None else 0
    newTweet['tw_user_id'] = tweet['user']['id_str']
    newTweet['query_id'] = queryId
    return newTweet

#Tweet raw data
def saveRawTweet(tweets):
    tweetJson = "TW_TWEET_BACKUP.json"
    with open(tweetJson, "a+") as data:
        for tweet in tweets:
            data.write(json.dumps(tweet, ensure_ascii=False).encode("utf-8"))
            data.write(os.linesep)

#User table
def saveUserFromTweet(user):
    userParquet = "TW_USER.parquet"
    if os.path.exists(userParquet):
        userBaseDF = spark.read.parquet(userParquet)
        existUser = userBaseDF.where(userBaseDF.id == user['id'])
        selectUserCol(user)
        if existUser.count() == 0:
           writeParquet(userParquet, [selectUserCol(user)], sc, spark)
    else:
        writeParquet(userParquet, [selectUserCol(user)], sc, spark)

def selectUserCol(user):
    newUser = {}
    newUser['id'] = user['id_str']
    newUser['name'] = user['name']
    newUser['screen_name'] = user['screen_name']
    return newUser

# def getTweetSchemaForDF():
    # schema = StructType([
    #             StructField("id", StringType(), True),
    #             StructField("created_at", StringType(), True),
    #             StructField("text", StringType(), True),
    #             StructField("hashtags", ArrayType(StringType()), True),
    #             StructField("geolocation", ArrayType(DoubleType()), True),
    #             StructField("favorite_count", LongType(), True),
    #             StructField("tw_user_id", StringType(), True),
    #             StructField("query_id", StringType(), True)
    #         ])
    # return schema

# def getUserSchemaForDF():
    # schema = StructType([
    #             StructField("id", StringType(), True),
    #             StructField("name", StringType(), True),
    #             StructField("screen_name", StringType(), True)
    #         ])
    # return schema


#########################################################################################################
