# -*- coding: utf-8 -*-
from geopy.geocoders import GoogleV3
import googlemaps
import json

# geolocator = GoogleV3(api_key="AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4")
# location = geolocator.geocode(query = "พารากอน", exactly_one=True, language="TH")
# test = json.loads(json.dumps(location.raw, ensure_ascii=False))
# print(test['place_id'])
# print

# location = geolocator.reverse(query="13.758564, 100.566482", exactly_one=True)
# print(location.address)
# print(location.raw)


gmaps = googlemaps.Client(key='AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4')
place = gmaps.places(query="สยามพารากอน")
place = json.loads(json.dumps(place, ensure_ascii=False))
print(place['results'][0]['geometry'])
# Geocoding an address
# geocode_result = gmaps.geocode('สนามหลวง')
# print(geocode_result)
