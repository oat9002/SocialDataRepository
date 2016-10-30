from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
import dateutil.parser as date

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

#Tweet table
def saveTweet(tweets):
    tweetParquet = "tweet.parquet"
    tweetBaseDF = spark.read.parquet(tweetParquet)
    tweetArr = []
    for tweet in tweets['tweets']:
        existTweet = tweetBaseDF.where(tweetBaseDF.id == tweet['id'])
        if existTweet.count() == 0:
            tweetArr.append(selectTweetCol(tweet))
            savePlaceFromTweet(tweet['place']) if tweet['place'] != None
            saveUserFromTweet(tweet['user'])
    tweetRDD = sc.parallelize(tweetArr)
    tweetDF = spark.read.json(tweetRDD)
    tweetDF.write.mode("append").parquet(tweetParquet)
    saveRawTweet(tweetArr)

def selectTweetCol(tweet):
    newTweet = {}
    newTweet['id'] = tweet['id']
    newTweet['created_at'] = date.parse(tweet['created_at'])
    newTweet['text'] = tweet['text']
    newTweet['hastags'] = tweet['entites']['hastags']
    newTweet['coordinates'] = tweet['coordinates']['coordinates'] if tweet['coordinates'] != "null" else None
    newTweet['place'] = tweet['place']['id'] if tweet['place'] != "null" else None
    newTweet['user'] = tweet['user']['id']
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

#TweetLocationSearch table

#########################################################################################################

#Place table
def savePlaceFromTweet(place):
    placeParquet = "tweetPlace.parquet"
    placeBaseDF = spark.read.parquet(placeParquet)
    existPlace = placeBaseDF.where(placeBaseDF.id == place['id'])
    if existPlace.count() == 0:
        placeRDD = sc.parallelize([selectPlaceCol(place)])
        placeDF = spark.read.json(placeRDD)
        placeDF.write.mode("append").parquet(placeParquet)

def selectPlaceCol(place):
    newPlace = {}
    newPlace['id'] = place['id']
    newPlace['name'] = place['name']
    newPlace['full_name'] = place['full_name']
    newPlace['coordinates'] = place['bounding_box']['coordinates']
    newPlace['country'] = palce['country']
    return newPlace


#########################################################################################################

#User table
def saveUserFromTweet(user):
    userParquet = "tweetUser.parquet"
    userBaseDF = spark.read.parquet(userParquet)
    existUser = userBaseDF.where(userBaseDF.id == user['id'])
    if existUser.count() == 0:
        userRDD = sc.parallelize([selectUserCol(user)])
        userDF = spark.read.json(userRDD)
        userDF.write.mode("append").parquet(userParquet)

def selectUserCol(user):
    newUser = {}
    newUser['id'] = user['id']
    newUser['name'] = user['name']
    newUser['screen_name'] = user['screen_name']
    return newUser

#########################################################################################################

#Test
def readTweet():
    tweetParquet = "tweetTest.parquet"
    tweetDF = spark.read.parquet(tweetParquet)
    tweetDF.createOrReplaceTempView("tweet")
    tweet = tweetDF.where(tweetDF.id_str['0'] == "790280483972194305")
    tweet.show()
    print tweet.select(tweet.text).collect()

#########################################################################################################



# def saveTweetQuery(tweetQuery):
#     tweetQueriesParquet = "tweetQuery.parquet"
#     tweetQueryBaseDF = spark.read.parquet(tweetQueriesParquet)
#     existTweetQuery = tweetQueryBaseDF.where(tweetQueryBaseDF.)
#     if existTweetQuery.count() == 0:
#         tweetQueryRDD = sc.parallelize([tweetQuery])
#         tweetQueryDF = spark.read.json(tweetQueryRDD)
#         tweetQueryDF.show()
