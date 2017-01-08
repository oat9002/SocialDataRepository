# -*- coding: utf-8 -*-
import sys
import os.path as path
sys.path.append('../service')
from pyspark import SparkContext
from pyspark.sql import *
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
    tweetArr = []
    if path.exists(tweetParquet):  
        tweetBaseDF = spark.read.parquet(tweetParquet)
        tweetArrBackup = []
        for tweet in tweets:
            existTweet = tweetBaseDF.where(tweetBaseDF.id == tweet['id'])
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
    writeParquet(tweetParquet, tweetArr)
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
    tweetJson = "TW_TWEET_BACKUP.json"
    with open(tweetJson, "a+") as data:
        for tweet in tweets:
            data.write(json.dumps(tweet, ensure_ascii=False).encode("utf-8"))
            data.write(os.linesep)

# def saveRawTweet(tweets):
#     tweetParquet = "rawTweet.parquet"
#     tweetBaseDF = spark.read.parquet(tweetParquet)
#     tweetArr = []
#     for tweet in tweets:
#         existTweet = tweetBaseDF.where(tweetBaseDF.id['0'] == tweet['id'])
#         if existTweet.count() == 0:
#             improveTweetCol(tweet)
#             normalizedTweet = json_normalize(tweet)
#             map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
#             tweetArr.append(normalizedTweet.to_json())
#     tweetRDD = sc.parallelize(tweetArr)
#     tweetDF = spark.createDataFrame(tweetRDD)
#     tweetDF.write.mode("append").parquet(tweetParquet)

#########################################################################################################

#User table
def saveUserFromTweet(user):
    userParquet = "TW_USER.parquet"
    if path.exists(userParquet): 
        userBaseDF = spark.read.parquet(userParquet)
        existUser = userBaseDF.where(userBaseDF.id == user['id'])
        if existUser.count() == 0:
            writeParquet(userParquet, [selectUserCol(user)])
    else:
        writeParquet(userParquet, [selectUserCol(user)])

def selectUserCol(user):
    newUser = {}
    newUser['id'] = user['id_str']
    newUser['name'] = user['name']
    newUser['screen_name'] = user['screen_name']
    return newUser

#########################################################################################################
