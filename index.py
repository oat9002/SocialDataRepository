# -*- coding: utf-8 -*-
from repository import SocialDataRepository
from flask import Flask, request
import json


app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/")
def index():
    return "Welcome to SocialDataRepository!"

@app.route("/saveQuery", methods=['POST'])
def saveQuery():
    SocialDataRepository.saveQuery(json.loads(request.get_data()))
    return ('', 204)

@app.route("/twitter/saveTweet", methods=['POST'])
def saveTweet():
    SocialDataRepository.saveTweet(json.loads(request.get_data()))
    return ('', 204)

@app.route("/twitter/read", methods=['GET'])
def readTweet():
    TwitterRepository.readTweet()
    return ('', 204)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
