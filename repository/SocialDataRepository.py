# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from geopy.distance import great_circle
from decimal import Decimal
import dateutil.parser as date
import uuid
import json
import TwitterRepository
import FoursquareRepository
import FacebookRepository
import sys
sys.path.append('../service')
from service.SocialDataService import writeParquet
import googlemaps
import os.path as path
from random import randint


spark = SparkSession\
    .builder\
    .appName("SocialDataRepository")\
    .getOrCreate()

sc = spark.sparkContext

# def writeParquet(parquetFile,rowArr):
#     if len(rowArr) > 0:
#         rowRDD = sc.parallelize(rowArr)
#         rowDF = spark.createDataFrame(rowRDD)
#         rowDF.write.mode("append").parquet(parquetFile)
#     else:
#         print("no row written")


#Twitter
def saveTweet(data):
    filteredTweet = TwitterRepository.saveTweet(data['tweets'], data['query']['id'])
    twitter = {}
    twitter['tweets'] = filteredTweet
    twitter['query'] = data['query']
    SocialDataNormalize("twitter", twitter)

#foursquare
def addFQVenue(data):
    venue = data['venue']
    place = {}
    place['keyword'] = venue['name']
    place['geolocation'] = str(venue['location']['lat'])+','+str(venue['location']['lng'])
    queryId = addPlaceOrQuery(place)
    FoursquareRepository.saveVenue(venue,queryId)
    FoursquareRepository.saveCategory(venue['categories'])

def addFQCheckin(data):
    checkin = data['hereNow']
    venueId = data['venueId']
    social = {}
    savedCheckin = FoursquareRepository.saveCheckin(checkin,venueId)
    social['checkin'] = savedCheckin
    social['place'] = FindPlaceByVenueId(venueId)
    if social['place'] != None:
        SocialDataNormalize("FQ_CHECKIN",social)

def addFQTips(data):
    tips = data['tips']
    venueId = data['venueId']
    social = {}
    allUser = []
    savedTips = FoursquareRepository.saveTips(tips,venueId)
    for tip in tips['items']:
        allUser.append(tip['user'])
    FoursquareRepository.saveUser(allUser)
    social['tips'] = savedTips
    social['place'] = FindPlaceByVenueId(venueId)
    if social['place'] != None:
        SocialDataNormalize("FQ_TIP",social)


def addFQPhotos(data):
    photos = data['photos']
    venueId = data['venueId']
    social = {}
    allUser = []
    savedPhotos = FoursquareRepository.savePhotos(photos,venueId)
    for photo in photos['items']:
        allUser.append(photo['user'])
    FoursquareRepository.saveUser(allUser)
    social['photos'] = savedPhotos
    social['place'] = FindPlaceByVenueId(venueId)
    if social['place'] != None:
        SocialDataNormalize("FQ_PHOTO",social)

def getAllFQVenue():
    return FoursquareRepository.getAllVenue()


#SOCIALDATA##################################
def addPlaceOrQuery(newPlace):
    #if no field 'geolocation'
    if not 'geolocation' in newPlace:
        #search for coordinate
        placeGoogle = getPlacesFromGoogle(newPlace['keyword'])
        if placeGoogle != None:
            placeParquet = "PLACE.parquet"
            placeDF = spark.read.parquet(placeParquet)
            places = placeDF.select(placeDF.id, placeDF.geolocation).collect()
            samePlace = False
            for place in places:
                samePlace = comparePlace(place, placeGoogle)
                if samePlace:
                    query = {}
                    query['keyword'] = newPlace['keyword']
                    queryid = saveQuery(query,  place['id'])
                    return queryid
            if not samePlace:
                randomIndex = randint(0, len(placeGoogle) - 1)
                place = {}
                place['name'] = placeGoogle[randomIndex]['name']
                coordinate = (str(placeGoogle[randomIndex]['geometry']['location']['lat']), (str(placeGoogle[randomIndex]['geometry']['location']['lng'])))
                place['geolocation'] = ",".join(coordinate)
                placeid = savePlace(place)
                query = {}
                query['keyword'] = newPlace['keyword']
                queryid = saveQuery(query,placeid)
                return queryid
        else:
            predictedPlaces = getPredictedPlaceFromGoogle(newPlace['keyword'])
            randomIndex = randint(0, len(predictedPlaces) - 1)
            place = {}
            place['name'] = predictedPlaces[randomIndex]['name']
            place['geolocation'] = str(predictedPlaces[randomIndex]['geometry']['location']['lat'] + "," + predictedPlaces[randomIndex]['geometry']['location']['lng'])
            placeid = savePlace(place)
            query = {}
            query['keyword'] = newPlace['keyword']
            queryid = saveQuery(query,placeid)
            return queryid
    else:
        place = {}
        place['name'] = newPlace['keyword']
        place['geolocation'] = newPlace['geolocation']
        placeid = savePlace(place)
        query = {}
        query['keyword'] = newPlace['keyword']
        queryid = saveQuery(query,placeid)
        return queryid

# old from saveQuery
#     placeParquet = "PLACE.parquet"
#             placeDF = spark.read.parquet(placeParquet)
#             places = placeDF.select(placeDF.id, placeDF.geolocation).collect()
#             samePlace = False
#             placeGoogle = getPlacesFromGoogle(query['keyword'])
#             place_id = ""
#             for place in places:
#                 samePlace = comparePlace(place, placeGoogle, query['keyword'])
#                 if samePlace:
#                     place_id = place['id']
#                     break
#             if samePlace:
#                 queries.append(createQuerySchema(query, place_id))
#             else:
#                 queries.append(createQuerySchema(query, None))
#             queryRDD = sc.parallelize([createQuerySchema(query)])
#             queryDF = spark.createDataFrame(queryRDD)
#             queryDF.write.mode("append").parquet(

def FindPlaceByVenueId(venueId):
    queryParquet = "QUERY.parquet"
    placeParquet = "PLACE.parquet"
    if path.exists(queryParquet) and path.exists(placeParquet):
        queryBaseDF = spark.read.parquet(queryParquet)
        placeBaseDF = spark.read.parquet(placeParquet)
        queryId = FoursquareRepository.findQueryIdByVenueId(venueId)
        if queryId != None:
            existQuery = queryBaseDF.where(queryBaseDF.id == queryId)
            if existQuery.count() >= 0:
                placeId = existQuery.first().place_id
                existPlace = placeBaseDF.where(placeBaseDF.id == placeId)
                if existPlace.count() >= 0:
                    if existPlace.first() != None:
                        return existPlace.first().asDict()
    return None

#SOCIALDATA table#########################################

def SocialDataNormalize(type, data):
    socialDataArr = []
    socialDataParquet = "SOCIALDATA.parquet"
    # if path.exists(socialDataParquet):
    #     socialDataBaseDF = spark.read.parquet(socialDataParquet)
    if type == "twitter":
        for tweet in data['tweets']:
            tweet['place_id'] = data['query']['place_id']
            normalizedData = createSocialDataSchema("twitter", tweet)
            socialDataArr.append(normalizedData)
    elif type == "FQ_TIP":
        for tip in data['tips']:
            newdata = {}
            newdata['tip'] = tip
            newdata['place'] = data['place']
            normalizedData = createSocialDataSchema(type, newdata)
            socialDataArr.append(normalizedData)
    elif type == "FQ_PHOTO":
        for photo in data['photos']:
            newdata = {}
            newdata['photo'] = photo
            newdata['place'] = data['place']
            normalizedData = createSocialDataSchema(type, newdata)
            socialDataArr.append(normalizedData)
    elif type == "FQ_CHECKIN":
        normalizedData = createSocialDataSchema(type, data)
        socialDataArr.append(normalizedData)
    # print(socialDataArr)
    writeParquet(socialDataParquet,socialDataArr, sc, spark, getSocialDataSchemaForDF())

def createSocialDataSchema(type, data):
    newData = {}
    newData['id'] = str(uuid.uuid4())

    if type == "twitter":
        newData['created_at'] = data['created_at']
        newData['geolocation'] = data['geolocation']
        newData['place_id'] = data['place_id']
        newData['message'] = data['text']
        newData['number_of_checkin'] = data['favorite_count']
        newData['source'] = "twitter"
        newData['source_id'] = data['id']
        return newData
    elif type == "FQ_TIP":
        newData['created_at'] = data['tip']['created_at']
        newData['geolocation'] = data['place']['geolocation']
        newData['place_id'] =data['place']['id']
        newData['message'] = data['tip']['message']
        newData['number_of_checkin'] = 1
        newData['source'] = type
        newData['source_id'] = data['tip']['tipid']
        return newData
    elif type == "FQ_PHOTO":
        newData['created_at'] = data['photo']['created_at']
        newData['geolocation'] = data['place']['geolocation']
        newData['place_id'] =data['place']['id']
        newData['message'] = data['photo']['photo']
        newData['number_of_checkin'] = 1
        newData['source'] = type
        newData['source_id'] = data['photo']['photoid']
        return newData
    elif type == "FQ_CHECKIN":
        newData['created_at'] = data['checkin']['created_at']
        newData['geolocation'] = data['place']['geolocation']
        newData['place_id'] =data['place']['id']
        newData['message'] = ""
        newData['number_of_checkin'] = data['checkin']['count']
        newData['source'] = type
        newData['source_id'] = data['checkin']['id']
        return newData
    return None

def getSocialDataSchemaForDF():
    schema = StructType([
                StructField("id", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("geolocation", ArrayType(DoubleType()), True),
                StructField("place_id", StringType(), True),
                StructField("message", StringType(), True),
                StructField("number_of_checkin", IntegerType(), True),
                StructField("source", StringType(), True),
                StructField("source_id", StringType(), True)
            ])
    return schema

#Query table#########################################
def saveQuery(query,place_id):
    queryParquet = "QUERY.parquet"
    if path.exists(queryParquet):
        queryBaseDF = spark.read.parquet(queryParquet)
        existQuery = queryBaseDF.where(queryBaseDF.keyword == query['keyword'])
        if existQuery.count() > 0:
            return existQuery.first().id
    newQuery = createQuerySchema(query,place_id)
    writeParquet(queryParquet,[newQuery], sc, spark, getQuerySchemaForDF())
    return newQuery['id']

def createQuerySchema(query, place_id):
    newQuery = {}
    newQuery['id'] = str(uuid.uuid4())
    newQuery['keyword'] = query['keyword']
    newQuery['frequency'] = 0
    newQuery['place_id'] = place_id
    return newQuery

def getQuerySchemaForDF():
    schema = StructType([
                StructField("id", StringType(), True),
                StructField("keyword", StringType(), True),
                StructField("frequency", IntegerType(), True),
                StructField("place_id", StringType(), True)
            ])
    return schema

#Place table###############################################
def savePlace(place):
    placeParquet = "PLACE.parquet"
    if path.exists(placeParquet):
        placeBaseDF = spark.read.parquet(placeParquet)
        placeLL = place['geolocation']
        allPlace = placeBaseDF.collect()
        for existPlace in allPlace:
            existLL = existPlace['geolocation']
            if compareLatLng(existLL[0],existLL[1],placeLL[0],placeLL[1]):
                return existPlace['id']
    newPlace = createPlaceSchema(place)
    writeParquet(placeParquet,[newPlace], sc, spark, getPlaceSchemaForDF())
    return newPlace['id']

def createPlaceSchema(place):
    newPlace = {}
    newPlace['id'] = str(uuid.uuid4())
    newPlace['name'] = place['name']
    newPlace['geolocation'] = place['geolocation']
    return newPlace

def getPlaceSchemaForDF():
    schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("geolocation", ArrayType(DoubleType()), True)
            ])
    return schema

def comparePlace(place_db, place_google): #place from database abd place from google
    geolo_db = place_db['geolocation']
    samePlace = False
    if place_google != None:
        for place in place_google:
            place = json.loads(json.dumps(place, ensure_ascii=False))
            samePlace = compareLatLng(place['geometry']['location']['lat'], place['geometry']['location']['lng'], geolo_db[0], geolo_db[1])
            if samePlace:
                break
    return samePlace
# compare 100 m.
def compareLatLng(lat1, lng1, lat2, lng2):
    newport_ri = (Decimal(lat1), Decimal(lng1))
    cleveland_oh = (Decimal(lat2), Decimal(lng2))
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
def getPredictedPlaceFromGoogle(name): #name of place is to predict
    gmaps = googlemaps.Client(key='AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4')
    names = gmaps.places_autocomplete(input_text=name, radius=100, components={'country': 'TH'})
    places = []
    for item in names:
        places.append(gmaps.place(place_id=item['place_id'])['result'])
    return places
