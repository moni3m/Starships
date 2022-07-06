# ---------------------------------TASK--------------------------------------------------------------

# The data in this database has been pulled from https://swapi.dev/. As well as 'people', the API has data on starships.
# In Python, pull data on all available starships from the API. The "pilots" key contains URLs pointing to the characters who pilot the starship.
# Use these to replace 'pilots' with a list of ObjectIDs from our characters collection,
# then insert the starships into their own collection.
# Use functions at the very least!

# ---------------------------------PLAN----------------------------------

# Create a function that will get starships data from the API
# Create a function that will loop through number of pages from the API collecting starship data from each page
# Create a function that checks if pilots are empty and retrieve the pilot's API call from each ship
# Create a function that takes searches for the corresponding pilot ID from the pilot API call
# Create a function that replaces the pilot API call in the starships to the pilot ID
# Create a function that inserts the starships data with the new information into a new collection

# --------------------------------IMPORTING LIBRARIES---------------------

import pymongo
import requests
from pprint import pprint

# ---------------------FUNCTIONS---------------------------


def drop_collection(collection_name: str, database_name: str):  # Function will drop a collection if it already exists
    client = pymongo.MongoClient("localhost:27017")  # Creating a pymongo client
    db = client[database_name]  # Creating a database with the inputted database name
    try:
        return db.drop_collection(collection_name)  # this will drop the collection
    except:
        print(f"{collection_name} collection could not be dropped.")

drop_collection("starships", "starwars")


def do_api_call(url: str):  # This function completes the API call request by inputting the URL as the argument
    results = requests.get(url).json()  # HTTP GET request from the given URL returning the results in a JSON format, storing it in a varibale called "response"
    return results  # returns only the result\'s dictionary from the API call back to the function


def collecting_ship_data():  # Function will loop through any number of pages, collecting the ship data from each page
    url = "https://swapi.dev/api/starships/?page=1"  # URL stores the url of the first page
    ship_info = []  # a list to store the ship data
    try:
        while requests.get(url).status_code == 200:  # a WHILE loop where condition is that the request is successful
            response = do_api_call(url)["results"]  # response variable stores only the list from 'results' from our call
            new_page = do_api_call(url)["next"]  # this variable stores the endpoint of our next page
            url = new_page  # this replaces the old url with the url of the 'new_page'
            for page in response:  # FOR loop iterates over every page in the response variable
                ship_info.append(page)  # the data from each page is added to the list 'ship_info'
    except:
        return ship_info  # list 'ship_info' is returned to the function to be used later


#pprint(collecting_ship_data())


def do_pilot_call():
    starships = collecting_ship_data()
    for ships in starships:
        if ships["pilots"]:
            print(ships["pilots"])

#do_pilot_call()

#def pilot_api_replacement():



#if __name__ = "__main__" :
    collection = "starships"
    database = "starwars"



