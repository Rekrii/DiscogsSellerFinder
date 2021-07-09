from flask import Flask
import sqlite3
import datetime
import os
import time
import threading
from DiscogsSellerFinder import DiscogsSellerFinder

app = Flask(__name__)

dateToday = datetime.datetime.today().date()
connection = sqlite3.connect("../listings.db")
cursor = connection.cursor() 

class FlaskServer(Flask):
    def __init__(self, *args, **kwargs):
        super(FlaskServer, self).__init__(*args, **kwargs)
        self.something = "woo"


app = FlaskServer(__name__)


@app.route('/')
@app.route('/index')
def index():
    return "Flask Root page."


@app.route('/highestsellers')
def highestSellers():
    data = dsf.list_highest_sellers()
    newItems = dsf.get_newly_listed_items()
    output = '<table style="border-collapse: collapse; width:100%"><tr><th>Seller</th><th>Location</th><th>Count</th><th>Items</th></tr>'
    for seller in data:
        if(data[seller]['item_count'] > 1):
            output += "<tr>"
            output += '<td style="padding-bottom: 1em;">'+'<a href="https://www.discogs.com/seller/' + str(data[seller]['seller_name']) + '/mywants?ev=hxiiw">'+str(data[seller]['seller_name']) + "</a></td>"+'<td style="padding-bottom: 1em;">'+str(data[seller]['seller_location']) + "</td>"+'<td style="padding-bottom: 1em;">' + str(data[seller]['item_count']) + "</td>"
            output += '<td style="padding-bottom: 1em;">'
            listedTitles = []
            for item in data[seller]['items']:
                if not(data[seller]['items'][item]['title'] in listedTitles):
                    itemIsNew = data[seller]['items'][item]['identifier_string'] in newItems
                    if itemIsNew: output += "<b style='color: rgb(255,165,0)'>"
                    output += data[seller]['items'][item]['title'] + " __ " + data[seller]['items'][item]['price'] +"<br>"
                    if itemIsNew: output += "</b>"
                    listedTitles.append(data[seller]['items'][item]['title'])
            output += "</td>"
    output += "</table>"
    return output


dsf = DiscogsSellerFinder()

# dsf.process_wantlist_file("../wantlist.csv")

x = threading.Thread(target=dsf.process_wantlist_file_loop, args=("../wantlist.csv", ), daemon=True)
x.start()


# FLASK_APP=ResultsFrontEnd.py python3 -m flask run --host=10.80.0.1 --port=5000