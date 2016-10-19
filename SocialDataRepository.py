from repository import TwitterRepository
from service import byteify
from flask import Flask, request
import json

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/")
def index():
    return "Welcome to SocialDataRepository!"

@app.route("/twitter/saveTweetQuery", methods=['POST'])
def saveTweetQuery():
    # print {"query":request.form['query']}
    # TwitterRepository.saveTweetQuery(request.form['query'])
    return ('', 204)

@app.route("/twitter/saveTweet", methods=['POST'])
def saveTweet():
    TwitterRepository.saveTweet(byteify.json_loads_byteified(request.data))
    return ('', 204)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
