# -*- coding: utf-8 -*-
# import sys
# sys.path.append('../repository')
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
import requests

spark = SparkSession\
    .builder\
    .appName("FacebookService")\
    .getOrCreate()

sc = spark.sparkContext

# print(SocialDataRepository.comparePlace("13.72767543677477","13.72767543677477","13.72767543677477","13.72767543677477"))

venueId = "4bea24b59fa3ef3bc91a80c9"

resp = requests.get('http://localhost:6003/foursquare/detail?venue_id='+venueId)
if resp.status_code != 200:
    # This means something went wrong.
    # raise ApiError('GET /tasks/ {}'.format(resp.status_code))
    print("Request Error code "+resp.status_code)
else:
    venue = resp.json()['response']['venue']
    SocialDataRepository.addFQVenue(venue)
    parquetFile = spark.read.parquet("PLACE.parquet")
    parquetFile.show()

ex ={}
ex['keyword'] = "abc"
print(SocialDataRepository.addPlaceOrQuery(ex))