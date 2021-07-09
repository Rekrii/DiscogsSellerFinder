import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
import datetime
import time
import os

class DiscogsSellerFinder():

    def process_wantlist_file(self, wantlist_path):
        dateToday = datetime.datetime.today().date()
        connection = sqlite3.connect("../listings.db")
        cursor = connection.cursor()

        df = pd.read_csv(wantlist_path)

        release_ids = df['release_id'].tolist()
        print("All release ids: ", release_ids)
        
        testRun = False

        newReleaseCount = 0
        
        for release in release_ids:
            
            release_url = 'https://www.discogs.com/sell/release/' + str(release) + '?sort=price%2Casc&limit=250&page=1'
            #release = "14914560"
            print("Processing release id: ", release)
        
            rows = cursor.execute("SELECT * FROM listings WHERE date_found = '{}' and release_id = '{}'".format(dateToday, release)).fetchall()
            if len(rows) > 0:
                print("  >Already done today: ", release)
                continue
            else:
                # If we haven't done it already, wait and then go 
                time.sleep(5)  # not in a rush, don't hit a rate limiter/be nice to the service

            content = requests.get(release_url).text
            with open("../pages/" + str(release) + ".txt", 'w') as f:
                f.write(content)
            # file_object =  open("pages/" + str(release) + ".txt", 'r')
            # content = file_object.read()

            soup = BeautifulSoup(content, 'html.parser')
            listingSoups = soup.find_all(attrs={"data-release-id": release})
            if len(listingSoups) == 0:
                print("  >No items found for: ", release)
                q1 = "INSERT INTO listings ('date_found','release_id','release_title','media_condition','sleeve_condition','seller_name','seller_location','seller_text','item_price') VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
                q1 = q1.format(
                    dateToday, 
                    release, 
                    "No items for sale",  "No items for sale",  "No items for sale",  "No items for sale",  "No items for sale",  "No items for sale", "No items for sale")
                if not testRun:
                    cursor.execute(q1)
                    connection.commit()

            for subHtml in listingSoups:
                dateFound = dateToday
                releaseId = release
                itemTitle = subHtml.find(class_="item_description").find("a").get_text()
                mediaCondition = subHtml.find(class_="item_condition").find(class_="media-condition-tooltip").get("data-condition")
                sleeveCondition = subHtml.find(class_="item_condition").find(class_="item_sleeve_condition")
                # We don't always have a sleeve condition 'item_sleeve_condition' element, so check if the element is not none
                # and if it is, set the condition to "No Sleeve"
                if sleeveCondition != None:
                    sleeveCondition = sleeveCondition.get_text()
                else:
                    sleeveCondition = "No Sleeve"
                sellerName = subHtml.find(class_="seller_info").find("a").get_text()
                sellerLoc = subHtml.find(class_="seller_info").find("span", text="Ships From:").next_sibling
                # As above, no seller location sometimes, make sure the element exists
                if sellerLoc != None:
                    sellerLoc = sellerLoc.strip()
                else:
                    sellerLoc = "No seller location"
                sellerText = subHtml.find(class_="item_description").find(class_="item_release_link hide_mobile").get_text()
                itemPrice = subHtml.find(class_="item_price").find("span").get_text()

                q1 = "INSERT INTO listings ('date_found','release_id','release_title','media_condition','sleeve_condition','seller_name','seller_location','seller_text','item_price') VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
                q1 = q1.format(
                    dateFound, 
                    releaseId, 
                    itemTitle.replace("'",""), 
                    mediaCondition.replace("'",""), 
                    sleeveCondition.replace("'",""), 
                    sellerName.replace("'",""), 
                    sellerLoc.replace("'",""), 
                    sellerText.replace("'",""),
                    itemPrice.replace("'",""))

                if testRun:
                    print(q1)
                else:
                    newReleaseCount = newReleaseCount + 1
                    cursor.execute(q1)
                    connection.commit()

        print("New listings added: ", newReleaseCount)
        return newReleaseCount


    def process_wantlist_file_loop(self, wantlist_path, post_run_sleep=12*60*60):
        while(True):
            self.process_wantlist_file(wantlist_path)
            # Convert 24h to seconds
            time.sleep(post_run_sleep)


    # Lots of horrible SQL selects here but they 'just work'
    def list_highest_sellers(self):
        dateToday = datetime.datetime.today().date()
        connection = sqlite3.connect("../listings.db")
        cursor = connection.cursor() 

        sellerCount = cursor.execute("SELECT seller_name, seller_location, COUNT(seller_name) as count FROM listings WHERE date_found = '{}' AND (sleeve_condition = 'Mint (M)' OR sleeve_condition = 'Near Mint (NM or M-)' OR sleeve_condition = 'Very Good Plus (VG+)') GROUP BY seller_name ORDER BY count DESC".format(dateToday)).fetchall()
        
        data = {}
        for seller in sellerCount:
            index = len(data)
            listedItems = cursor.execute("SELECT release_title, media_condition, sleeve_condition, item_price, identifier_string FROM listings WHERE date_found = '{}' AND seller_name = '{}'".format(dateToday, seller[0])).fetchall()
            uniqueItems = cursor.execute("SELECT DISTINCT release_title FROM listings WHERE date_found = '{}' AND seller_name = '{}' AND (sleeve_condition = 'Mint (M)' OR sleeve_condition = 'Near Mint (NM or M-)' OR sleeve_condition = 'Very Good Plus (VG+)')".format(dateToday, seller[0])).fetchall()
            data[index] = {'seller_name': seller[0], "seller_location": seller[1], "item_count": len(uniqueItems)}
            data[index]['items'] = {}
            for item in listedItems:
                data[index]['items'][len(data[index]['items'])] = {'title': item[0], "media_condition": item[1], "sleeve_condition": item[2], "price": item[3], "identifier_string": item[4]}

        return data


    def get_newly_listed_items(self, olderDate = None, newerDate = None):
        dateToday = datetime.datetime.today().date()
        if newerDate == None:
            newerDate = dateToday
        if olderDate == None:
            olderDate = newerDate - datetime.timedelta(days=1)

        newQuery = "SELECT  lis.identifier_string                                                   \
                    FROM    (SELECT * FROM listings WHERE date_found = '{}') lis    \
                    WHERE   lis.identifier_string NOT IN                            \
                    (                                                               \
                    SELECT  identifier_string                                       \
                    FROM    (SELECT * FROM listings WHERE date_found = '{}') r      \
                    )".format(newerDate, olderDate)

        connection = sqlite3.connect("../listings.db")
        cursor = connection.cursor() 

        newItemsDict = cursor.execute(newQuery).fetchall()
        newItemsList = []
        for item in newItemsDict:
            newItemsList.append(item[0])
        return newItemsList













            # findExistingQuery = "SELECT * FROM listings WHERE date_found = '{}' AND release_id = '{}' AND release_title = '{}' AND media_condition = '{}' AND sleeve_condition = '{}' AND seller_name = '{}' AND seller_location = '{}' AND seller_text = '{}' AND item_price = '{}'"
            # findExistingQuery = findExistingQuery.format(
            #     dateFound, 
            #     releaseId, 
            #     itemTitle.replace("'",""), 
            #     mediaCondition.replace("'",""), 
            #     sleeveCondition.replace("'",""), 
            #     sellerName.replace("'",""), 
            #     sellerLoc.replace("'",""), 
            #     sellerText.replace("'",""),
            #     itemPrice.replace("'",""))
            # rows = cursor.execute(findExistingQuery).fetchall()
            # if len(rows) > 0:
            #     print("  >Listing already in the db: ", release)
            #     continue
            # else:
            #     # If we haven't done it already, wait and then go 
            #     time.sleep(5)  # not in a rush, don't hit a rate limiter/be nice to the service