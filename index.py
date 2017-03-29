# -*- coding: utf-8 -*-
from repository import SocialDataRepository
from flask import Flask, request, jsonify
import json


app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/")
def index():
    return "Welcome to SocialDataRepository!"

@app.route("/addPlaceOrQuery", methods=['POST']) #{keyword: , geolocation:}
def addPlaceOrQuery():
    SocialDataRepository.addPlaceOrQuery(json.loads(request.get_data()))
    return ('', 204)

@app.route("/twitter/addTweet", methods=['POST'])
def addTweet():
    twitter = json.loads(request.get_data())
    for tw in twitter['twitter']:
        SocialDataRepository.saveTweet(tw)
    # SocialDataRepository.saveTweet(request.get_json(force=True))
    # print json.dumps(request.get_data(), indent=2)
    return ('', 204)

@app.route("/foursquare/addVenue", methods=['POST'])
def addFQVenue():
    SocialDataRepository.addFQVenue(json.loads(request.get_data()))
    return ('', 204)

@app.route("/foursquare/addCheckin", methods=['POST'])
def addFQCheckin():
    SocialDataRepository.addFQCheckin(json.loads(request.get_data()))
    return ('', 204)

@app.route("/foursquare/addTips", methods=['POST'])
def addFQTips():
    SocialDataRepository.addFQTips(json.loads(request.get_data()))
    return ('', 204)

@app.route("/foursquare/addPhotos", methods=['POST'])
def addFQPhotos():
    SocialDataRepository.addFQPhotos(json.loads(request.get_data()))
    return ('', 204)

@app.route("/foursquare/getAllVenue", methods=['GET'])
def getAllFQVenue():
    res = SocialDataRepository.getAllFQVenue()
    venue = {}
    venue['venues'] = res
    return jsonify(venue)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5002, threaded=True)
