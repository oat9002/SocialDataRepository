# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
import uuid
import os.path as path
import datetime
import dateutil.parser as date


spark = SparkSession\
    .builder\
    .appName("FoursquareRepository")\
    .getOrCreate()

sc = spark.sparkContext

def writeParquet(parquetFile,rowArr):
    rowRDD = sc.parallelize(rowArr)
    rowDF = spark.createDataFrame(rowRDD)
    rowDF.write.mode("append").parquet(parquetFile)

#FQ_VENUE####################################
def saveVenue(venue,queryId):
    venueParquet = "FQ_VENUE.parquet"
    if path.exists(venueParquet):     
        venueBaseDF = spark.read.parquet(venueParquet)
        existVenue = venueBaseDF.where(venueBaseDF.venueid == venue['id'])
        if existVenue.count() == 0:
            writeParquet(venueParquet,[selectVenueCol(venue,queryId)])
    else:
        writeParquet(venueParquet,[selectVenueCol(venue,queryId)])            

def selectVenueCol(venue,queryId):
    newVenue = {}
    newVenue['venueid'] = venue['id']
    newVenue['query_id'] = queryId
    newVenue['geolocation'] = str(venue['location']['lat'])+','+str(venue['location']['lng'])
    newVenue['category'] = venue['categories'][0]['name']
    return newVenue

#FQ_CHECKIN####################################
def saveCheckin(checkin,venueId):
    checkinParquet = "FQ_CHECKIN.parquet"
    writeParquet(checkinParquet,[selectCheckinCol(checkin,queryId)])             

def selectCheckinCol(checkin,venueId):
    newCheckin = {}
    newCheckin['id'] = str(uuid.uuid4())
    newCheckin['datetime'] = datetime.datetime.now().isoformat() 
    newCheckin['count'] = checkin['count']
    newCheckin['venueid'] = venueId
    return newCheckin

#FQ_TIP####################################
def saveTip(tips,venueId):
    tipParquet = "FQ_TIP.parquet"
    allTips = []
    if path.exists(tipParquet):     
        tipBaseDF = spark.read.parquet(tipParquet)
        for tip in tips['items']:
            existTip = tipBaseDF.where(tipBaseDF.tipid == tip['id'])
            if existTip.count() == 0:
                allTips.append(selectTipCol(tip,venueId))
        writeParquet(checkinParquet,allTips)     
    else:
        for tip in tips['items']:
            allTips.append(selectTipCol(tip,venueId))
        writeParquet(tipParquet,allTips)            

def selectTipCol(tip,venueId):
    newTip = {}
    newTip['tipid'] = tip['id']
    newTip['datetime'] =  datetime.datetime.fromtimestamp(tip['createdAt']).isoformat()
    newTip['message'] = tip['text']
    newTip['venueid'] = venueId
    newTip['userid'] = tip['user']['id']
    return newTip

#FQ_USER####################################
def saveUser(user):
    userParquet = "FQ_USER.parquet"
    if path.exists(userParquet):     
        userBaseDF = spark.read.parquet(userParquet)
        existUser = userBaseDF.where(userBaseDF.userid == user['id'])
        if existUser.count() == 0:
            writeParquet(userParquet,[selectUserCol(user)])
    else:
        writeParquet(userParquet,[selectUserCol(user)])            

def selectUserCol(user):
    newUser = {}
    newUser['userid'] = user['id']
    newUser['name'] = user['firstName']+' '+user['lastName']
    newUser['gender'] = user['gender']
    return newUser

#FQ_PHOTO####################################
def savePhoto(photos,venueId):
    photoParquet = "FQ_PHOTO.parquet"
    allPhoto = []
    if path.exists(photoParquet):     
        photoBaseDF = spark.read.parquet(photoParquet)
        for photo in photos['items']:
            existPhoto = photoBaseDF.where(photoBaseDF.photoid == photo['id'])
            if existPhoto.count() == 0:
                allPhoto.append(selectPhotoCol(photo,venueId))
        writeParquet(photoParquet,allPhoto)     
    else:
        for photo in photos['items']:
            allPhoto.append(selectPhotoCol(photo,venueId))
        writeParquet(photoParquet,allPhoto)            

def selectPhotoCol(photo,venueId):
    newPhoto = {}
    newPhoto['photoid'] = photo['id']
    newPhoto['datetime'] =  datetime.datetime.fromtimestamp(photo['createdAt']).isoformat()
    newPhoto['photo'] = photo['prefix']+photo['suffix']
    newPhoto['venueid'] = venueId
    newPhoto['userid'] = photo['user']['id']
    return newTip
