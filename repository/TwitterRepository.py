# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
import dateutil.parser as date
import uuid


spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

#TW_TWEET table
def saveTweet(tweets, queryId):
    tweetParquet = "TW_TWEET.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    tweetArr = []
    for tweet in tweets:
        existTweet = tweetBaseDF.where(tweetBaseDF.id == tweet['id'])
        if existTweet.count() == 0:
            tweetArr.append(selectTweetCol(tweet, queryId))
            saveUserFromTweet(tweet['user'])
    tweetRDD = sc.parallelize(tweetArr)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("append").parquet(tweetParquet)
    saveRawTweet(tweetArr)
    return tweetArr #for saving to Social table

def selectTweetCol(tweet, queryId):
    newTweet = {}
    newTweet['id'] = tweet['id_str']
    newTweet['created_at'] = date.parse(tweet['created_at'])
    newTweet['text'] = tweet['text']
    newTweet['hashtags'] = tweet['entities']['hashtags']
    newTweet['geolocation'] = tweet['coordinates']['coordinates'] if tweet['coordinates'] != None else None
    newTweet['favorite_count'] = tweet['favorite_count'] if tweet['favorited'] != False or  tweet['favorited'] != None else None
    newTweet['tw_user_id'] = tweet['user']['id_str']
    newTweet['query_id'] = queryId
    return newTweet

#Tweet raw data
def saveRawTweet(tweets):
    tweetParquet = "rawTweet.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    tweetArr = []
    for tweet in tweets:
        existTweet = tweetBaseDF.where(tweetBaseDF.id['0'] == tweet['id'])
        if existTweet.count() == 0:
            improveTweetCol(tweet)
            normalizedTweet = json_normalize(tweet)
            map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
            tweetArr.append(normalizedTweet.to_json())
    tweetRDD = sc.parallelize(tweetArr)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("append").parquet(tweetParquet)

#########################################################################################################

#User table
def saveUserFromTweet(user):
    userParquet = "TW_USER.parquet"
    userBaseDF = spark.read.parquet(userParquet)
    existUser = userBaseDF.where(uszerBaseDF.id == user['id'])
    if existUser.count() == 0:
        userRDD = sc.parallelize([selectUserCol(user)])
        userDF = spark.read.json(userRDD)
        userDF.write.mode("append").parquet(userParquet)

def selectUserCol(user):
    newUser = {}
    newUser['id'] = user['id_str']
    newUser['name'] = user['name']
    newUser['screen_name'] = user['screen_name']
    return newUser

#########################################################################################################
