# -*- coding: utf-8 -*-
import sys
import os
sys.path.append('../service')
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from service.SocialDataService import writeParquetWithSchema
from service.SocialDataService import writeParquet
from geopy.distance import great_circle
import dateutil.parser as date
import uuid
import json
import pydoop.hdfs

hdfs = pydoop.hdfs.hdfs()

#TW_TWEET table
def saveTweet(tweets, queryId, sc, spark): #queryId is packed with Tweet Data
    tweetParquet = "hdfs://stack-02:9000/SocialDataRepository/TW_TWEET.parquet"
    userParquet = "hdfs://stack-02:9000/SocialDataRepository/TW_USER.parquet"
    tweetArr = []
    if hdfs.exists(tweetParquet):
        tweetBaseDF = spark.read.parquet(tweetParquet)
        tweetArrBackup = []
        for tweet in tweets:
            existTweet = tweetBaseDF.where(tweetBaseDF.id == tweet['id_str'])
            if existTweet.count() == 0:
                tweetArr.append(selectTweetCol(tweet, queryId))
                saveUserFromTweet(tweet['user'], sc, spark)
                tweetArrBackup.append(tweet)
        saveRawTweet(tweetArrBackup)
    else:
        for tweet in tweets:
            tweetArr.append(selectTweetCol(tweet, queryId))
            saveUserFromTweet(tweet['user'], sc, spark)
        saveRawTweet(tweets)
    #writeParquetWithSchema(tweetParquet, tweetArr, getTweetSchemaForDF(), sc, spark)
    writeParquet(tweetParquet, tweetArr, sc, spark)
    return tweetArr #for saving to Social table


def selectTweetCol(tweet, queryId):
    newTweet = {}
    newTweet['id'] = tweet['id_str']
    newTweet['created_at'] = date.parse(tweet['created_at']).isoformat()
    newTweet['text'] = tweet['text']
    hashtags = ['']
    if tweet['entities']['hashtags'] != None:
        for ht in tweet['entities']['hashtags']:
            hashtags.append(ht['text'])
    newTweet['hashtags'] = hashtags
    newTweet['geolocation'] = tweet['coordinates']['coordinates'] if tweet['coordinates'] != None else ['']
    newTweet['favorite_count'] = tweet['favorite_count'] if tweet['favorited'] != False or  tweet['favorited'] != None else 0
    newTweet['tw_user_id'] = tweet['user']['id_str']
    newTweet['query_id'] = queryId
    tweetRow = Row(id=newTweet['id'], created_at=newTweet['created_at'], text=newTweet['text'], hashtags=newTweet['hashtags'], geolocation=newTweet['geolocation'], favorite_count=newTweet['favorite_count'], tw_user_id=newTweet['tw_user_id'], query_id=newTweet['query_id'])
    return tweetRow

#Tweet raw data
def saveRawTweet(tweets):
    tweetJson = "TW_TWEET_BACKUP.json"
    with open(tweetJson, "a+") as data:
        for tweet in tweets:
            data.write(json.dumps(tweet, ensure_ascii=False).encode("utf-8"))
            data.write(os.linesep)

#User table
def saveUserFromTweet(user, sc, spark):
    userParquet = "hdfs://stack-02:9000/SocialDataRepository/TW_USER.parquet"
    if hdfs.exists(userParquet):
        userBaseDF = spark.read.parquet(userParquet)
        existUser = userBaseDF.where(userBaseDF.id == user['id'])
        if existUser.count() == 0:
            #writeParquetWithSchema(userParquet, [selectUserCol(user)], getUserSchemaForDF(), sc, spark)
            writeParquet(userParquet, [selectUserCol(user)], sc, spark)
    else:
        #writeParquetWithSchema(userParquet, [selectUserCol(user)], getUserSchemaForDF(), sc, spark)
        writeParquet(userParquet, [selectUserCol(user)], sc, spark)
def selectUserCol(user):
    newUser = Row(id=user['id_str'], name=user['name'],screen_name=user['screen_name'])
    #newUser['id'] = user['id_str']
    #newUser['name'] = user['name']
    #newUser['screen_name'] = user['screen_name']
    return newUser

def getTweetSchemaForDF():
    schema = StructType([
                StructField("id", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("text", StringType(), True),
                StructField("hashtags", ArrayType(StringType()), True),
                StructField("geolocation", ArrayType(StringType()), True),
                StructField("favorite_count", LongType(), True),
                StructField("tw_user_id", StringType(), True),
                StructField("query_id", StringType(), True)
            ])
    return schema

def getUserSchemaForDF():
    schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("screen_name", StringType(), True)
            ])
    return schema


#########################################################################################################
