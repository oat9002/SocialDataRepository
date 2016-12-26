# -*- coding: utf-8 -*-
from geopy.geocoders import GoogleV3

geolocator = GoogleV3()
location = geolocator.geocode(query = "สยาม", region = "66")
print(location.address)
print(location.raw)
print
location = geolocator.reverse(query="13.758564, 100.566482", exactly_one=True)
print(location.address)
print(location.raw)
