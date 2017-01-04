# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
from decimal import Decimal
import dateutil.parser as date
import uuid
import TwitterRepository
import FoursquareRepository
import FacebookRepository
import googlemaps
import json


spark = SparkSession\
    .builder\
    .appName("SocialDataRepository")\
    .getOrCreate()

sc = spark.sparkContext

#Twitter
def saveTweet(data):
    filteredTweet = TwitterRepository.saveTweet(data['tweets'], data['query']['id'])
    twitter = {}
    twitter['tweets'] = filteredTweet
    twitter['query'] = data['query']
    SocialDataNormalize("twitter", twitter)

#foursquare
def addFQVenue(venue):


#SOCIALDATA##################################
def SocialDataNormalize(type, data):
    socialDataArr = []
    if type == "twitter":
        socialDataParquet = "SOCIALDATA.parquet"
        socialDataBaseDF = spark.read.parquet(socialDataParquet)
        for tweet in data['tweets']:
            normalizedData = createSocialDataSchema("twitter", tweet)
            socialDataArr.append(normalizedData)
    socialDataRDD = sc.parallelize(socialDataArr)
    socialDataDF = spark.createDataFrame(socialDataRDD)
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

#Query table#########################################
def saveQuery(query):
    queryParquet = "QUERY.parquet"
    queryBaseDF = spark.read.parquet(queryParquet)
    existQuery = queryBaseDF.where(queryBaseDF.keyword == query['keyword']).select(queryBaseDF.id)
    if existQuery.count() == 0:
        placeParquet = "PLACE.parquet"
        placeDF = spark.read.parquet(placeParquet)
        places = placeDF.select(placeDF.id, placeDF.geolocation).collect()
        samePlace = False
        placeGoogle = getPlacesFromGoogle(query['keyword'])
        place_id = ""
        for place in places:
            samePlace = compareQueryAndPlace(place, placeGoogle, query['keyword'])
            if samePlace:
                place_id = place['id']
                break
        if samePlace:
            queries.append(createQuerySchema(query, place_id))
        else:
            queries.append(createQuerySchema(query, None))
        queryRDD = sc.parallelize([createQuerySchema(query)])
        queryDF = spark.createDataFrame(queryRDD)
        queryDF.write.mode("append").parquet(queryParquet)

def createQuerySchema(query, place_id):
    querySchema = {}
    querySchema['id'] = str(uuid.uuid4())
    querySchema['keyword'] = query['keyword']
    querySchema['frequency'] = 0
    querySchema['place_id'] = place_id
    return querySchema

#Place table###############################################
def savePlace(place):
    placeParquet = "PLACE.parquet"
    placeBaseDF = spark.read.parquet(placeParquet)
    existPlace = placeBaseDF.where(placeBaseDF.id == place['id']).select(placeBaseDF.id)
    if existPlace.count() == 0:
        placeRDD = sc.parallelize([createPlaceSchema(place)])
        placeDF = spark.createDataFrame(placeRDD)
        placeDF.write.mode("append").parquet(placeParquet)

def createPlaceSchema(place):
    newPlace = {}
    newPlace['id'] = str(uuid.uuid4())
    newPlace['name'] = place['name']
    newPlace['geolocation'] = place['geolocation']
    return newPlace

def compareQueryAndPlace(place_db, place_google, query):
    geolo_db = place_db['geolocation'].split(",")
    samePlace = False
    if place_google != None:
        for place in place_google:
            place = json.loads(json.dumps(place, ensure_ascii=False))
            samePlace = comparePlace(place['geometry']['location']['lat'], place['geometry']['location']['lng'], geolo_db[0], geolo_db[1])
            if samePlace:
                break
    return samePlace
# compare 100 m. 
def comparePlace(lat1, lng1, lat2, lng2):
    newport_ri = (Decimal(format(lat1, ".6f")), Decimal(format(lng1, ".6f")))
    cleveland_oh = (Decimal(format(lat2, ".6f")), Decimal(format(lng2, ".6f")))
    acceptRadius = great_circle(newport_ri, cleveland_oh).miles
    if acceptRadius <= 0.0621371192:
        return True
    else:
        return False

def getPlacesFromGoogle(name):
    gmaps = googlemaps.Client(key='AIzaSyB8wgqC986H29FW0TTXRYJNwJLuIKVqVo0', retry_timeout=15)
    places = gmaps.places(query=name)
    if places['status'] == "OK":
        return places['results']
    else:
        return None
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
#             newLocationDF = spark.createDataFrame(newLocationRDD)
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
