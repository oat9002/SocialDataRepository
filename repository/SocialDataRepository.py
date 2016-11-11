# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
import dateutil.parser as date
import uuid
import TwitterRepository


spark = SparkSession\
    .builder\
    .appName("SocialDataRepository")\
    .getOrCreate()

sc = spark.sparkContext

def saveTweet(data):
    filteredTweet = TwitterRepository.saveTweet(data['tweets'], data['query']['id'])
    twitter = {}
    twitter['tweets'] = filteredTweet
    twitter['query'] = data['query']
    SocialDataNormalize("twitter", twitter)

def SocialDataNormalize(type, data):
    if type == "twitter":
        socialDataParquet = "SOCIALDATA.parquet"
        socialDataBaseDF = spark.read.parquet(socialDataParquet)
        socialDataArr = []
        for tweet in data['tweets']:
            normalizedData = createSocialDataSchema("twitter", tweet)
            socialDataArr.append(normalizedData)
        socialDataRDD = sc.parallelize(socialDataArr)
        socialDataDF = spark.read.json(socialDataRDD)
        socialDataDF.write.mode("append").parquet(socialDataParquet)

def createSocialDataSchema(type, data):
    if type == "twitter":
        newData = {}
        newData['id'] = uuid.uuid4()
        newData['created_at'] = date.parse(data['created_at'])
        newData['day_of_week'] = date.parse(data['created_at']).strftime("%A")
        newData['geolocation'] = None
        newData['place_name'] = None
        newData['message'] = data['text']
        newData['number_of_checkin'] = data['favorite_count']
        newData['source'] = "twitter"
        newData['source_id'] = data['id']
        return newData

#Query table
def saveQuery(query):
    queryParquet = "QUERY.parquet"
    queryBaseDF = spark.read.parquet(queryParquet)
    existQuery = queryBaseDF.where(queryBaseDF.keyword == query['keyword']).select(queryBaseDF.id)
    if existQuery.count() == 0:
        queryRDD = sc.parallelize([createQuerySchema(query)])
        queryDF = spark.read.json(queryRDD)
        queryDF.write.mode("append").parquet(queryParquet)

def createQuerySchema(query):
    querySchema = {}
    querySchema['id'] = str(uuid.uuid4())
    querySchema['keyword'] = query['keyword']
    querySchema['frequency'] = 0
    querySchema['place_id'] = query['place_id']
    newport_ri = (location['coordinates'][0], location['coordinates'][1])
    cleveland_oh = (row['coordinates'][0], ro['coordinates'][1])
    acceptRadius = great_circle(newport_ri, cleveland_oh).kilometers
    if acceptRadius <= 0.5:
        existLocation = True
    return querySchema

#########################################################################################################

#Place table
def savePlace(place):
    placeParquet = "PLACE.parquet"
    placeBaseDF = spark.read.parquet(placeParquet)
    existPlace = placeBaseDF.where(placeBaseDF.id == place['id']).select(placeBaseDF.id)
    if existPlace.count() == 0:
        placeRDD = sc.parallelize([createPlaceSchema(place)])
        placeDF = spark.read.json(placeRDD)
        placeDF.write.mode("append").parquet(placeParquet)

def createPlaceSchema(place):
    newPlace = {}
    newPlace['id'] = str(uuid.uuid4())
    newPlace['name'] = place['name']
    newPlace['geolocation'] = place['geolocation']
    return newPlace



#########################################################################################################

# #TweetLocationSearch table
# def saveTweetLocationSearch(location):
#     tweetLocationSearchParquet = "tweetLocationSearch.parquet"
#     tweetLocationSearchBaseDF = spark.read.parquet(tweetLocationSearchParquet)
#     existLocation = False
#     for row in tweetLocationSearchBaseDF.collect():
#         newport_ri = (location['coordinates'][0], location['coordinates'][1])
#         cleveland_oh = (row['coordinates'][0], ro['coordinates'][1])
#         acceptRadius = great_circle(newport_ri, cleveland_oh).kilometers
#         if acceptRadius <= 0.5:
#             existLocation = True
#             return row['id']
#         if !existLocation:
#             newLocationRDD = sc.parallelize([createTweetLocationSearchSchema(location)])
#             newLocationDF = spark.read.json(newLocationRDD)
#             newLocation.write.mode("append").parquet(tweetLocationSearchParquet)
#             return newLocation['id']
#
# def createTweetLocationSearchSchema(location):
#     tweetLocationSearch = {}
#     tweetLocationSearch['id'] = uuid.uuid4()
#     tweetLocationSearch['coordinates'] = location['coordinates'] if location['coordinates'] != "null" else None
#     tweetLocationSearch['frequency'] = 0
#     return tweetLocationSearch
#


#########################################################################################################
