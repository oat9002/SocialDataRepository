# -*- coding: utf-8 -*-
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
from repository import FoursquareRepository
from repository import SocialDataRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import os.path as path
import datetime

spark = SparkSession\
    .builder\
    .appName("Initial FoursquareRepository")\
    .getOrCreate()

sc = spark.sparkContext

with open('fqVenue.json') as json_data:
    venueJSON = json.load(json_data)
    FoursquareRepository.saveVenue(venueJSON['response']['venue'], 12453)

    parquetFile = spark.read.parquet("FQ_VENUE.parquet")
    parquetFile.show()

with open('fqCheckin.json') as json_data:
    checkinJSON = json.load(json_data)
    FoursquareRepository.saveCheckin(checkinJSON['response']['hereNow'], "4bea24b59fa3ef3bc91a80c9")

    parquetFile = spark.read.parquet("FQ_CHECKIN.parquet")
    parquetFile.show()


with open('fqTip.json') as json_data:
    tipJSON = json.load(json_data)
    FoursquareRepository.saveTip(tipJSON['response']['tips'], "4bea24b59fa3ef3bc91a80c9")

    parquetFile = spark.read.parquet("FQ_TIP.parquet")
    parquetFile.show()

print(datetime.datetime.fromtimestamp(1423633830).isoformat())

