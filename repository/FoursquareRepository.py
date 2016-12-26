# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
import dateutil.parser as date
import uuid
import os.path as path


spark = SparkSession\
    .builder\
    .appName("FoursquareRepository")\
    .getOrCreate()

sc = spark.sparkContext


#FQ_VENUE####################################
def saveVenue(venue,queryId):

    venueParquet = "FQ_VENUE.parquet"
    if path.exists(venueParquet):     
        venueBaseDF = spark.read.parquet(venueParquet)
        existVenue = venueBaseDF.where(venueBaseDF.venueid == venue['id'])
        if existVenue.count() == 0:
            venueRDD = sc.parallelize([selectVenueCol(venue,queryId)])
            venueDF = spark.createDataFrame(venueRDD)
            venueDF.write.mode("append").parquet(venueParquet)
    else:
        venueRDD = sc.parallelize([selectVenueCol(venue,queryId)])
        venueDF = spark.createDataFrame(venueRDD)
        venueDF.write.parquet(venueParquet)            

def selectVenueCol(venue,queryId):
    newVenue = {}
    newVenue['venueid'] = venue['id']
    newVenue['query_id'] = queryId
    newVenue['geolocation'] = str(venue['location']['lat'])+','+str(venue['location']['lng'])
    newVenue['category'] = venue['categories'][0]['name']
    return newVenue

