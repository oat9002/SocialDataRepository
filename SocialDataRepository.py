from repository import TwitterRepository
from flask import Flask, request
app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/")
def index():
    return "Welcome to SocialDataRepository!"

@app.route("/twitter/saveTweetQuery", methods=['POST'])
def saveTweetQuery():
    TwitterRepository.saveTweetQuery({"query": str(request.form['query'])})
    return ('', 204)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
