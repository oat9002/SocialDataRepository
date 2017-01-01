# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
from decimal import Decimal
import dateutil.parser as date
import uuid
import TwitterRepository
import googlemaps
import json


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

def createQuerySchema(query, place_id):
    querySchema = {}
    querySchema['id'] = str(uuid.uuid4())
    querySchema['keyword'] = query['keyword']
    querySchema['frequency'] = 0
    querySchema['place_id'] = place_id
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

def compareQueryAndPlace(place_db, query):
    gmaps = googlemaps.Client(key='AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4')
    places = gmaps.places(query=query)
    geolo_db = place_db['geolocation'].split(",")
    samePlace = False
    for place in places:
        place = json.loads(json.dumps(place, ensure_ascii=False))
        newport_ri = (Decimal(place['geometry']['lat']), Decimal(place['geometry']['lng']))
        cleveland_oh = (Decimal(geolo_db[0]), Decimal(geolo_db[1]))
        acceptRadius = great_circle(newport_ri, cleveland_oh).meters
        if acceptRadius <= 0.3:
            samePlace = True
            break
    return samePlace
        

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
