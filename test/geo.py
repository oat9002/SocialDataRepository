# -*- coding: utf-8 -*-
from geopy.geocoders import GoogleV3
import googlemaps
import json
from geopy.distance import great_circle
from decimal import Decimal

# geolocator = GoogleV3(api_key="AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4")
# location = geolocator.geocode(query = "พารากอน", exactly_one=True, language="TH")
# test = json.loads(json.dumps(location.raw, ensure_ascii=False))
# print(test['place_id'])
# print

# location = geolocator.reverse(query="13.758564, 100.566482", exactly_one=True)
# print(location.address)
# print(location.raw)


gmaps = googlemaps.Client(key='AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4')
test = gmaps.places_autocomplete(input_text="kmitl", radius=100, components={'country': 'TH'})
for i in test:
    print i['place_id']
    print gmaps.place(place_id=i['place_id'])['result']['name']
    print gmaps.place(place_id=i['place_id'])['result']['geometry']
# places = gmaps.places(query=test[2]['description'])
# print places['results'][0]['name']
# for place in places['results']:
#     place = json.loads(json.dumps(place, ensure_ascii=False))
#     newport_ri = (Decimal(format(place['geometry']['location']['lat'], ".6f")), Decimal(format(place['geometry']['location']['lng'], ".6f")))
#     cleveland_oh = (Decimal(13.734760), Decimal(100.777690))
#     # print("map: ", Decimal(format(place['geometry']['location']['lat'], ".6f")), ", ",Decimal(format(place['geometry']['location']['lng'], ".6f")))
#     # print("ori: ", Decimal(13.746180), ", ", Decimal(100.534201))
#     acceptRadius = great_circle(newport_ri, cleveland_oh).miles
#     print("acceptRadius: ", acceptRadius)

# Geocoding an address
# geocode_result = gmaps.geocode('สนามหลวง')
# print(geocode_result)
