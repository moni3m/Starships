# ---------------------------------TASK--------------------------------------------------------------

# The data in this database has been pulled from https://swapi.dev/. As well as 'people', the API has data on starships.
# In Python, pull data on all available starships from the API. The "pilots" key contains URLs pointing to the characters who pilot the starship.
# Use these to replace 'pilots' with a list of ObjectIDs from our characters collection,
# then insert the starships into their own collection.
# Use functions at the very least!

# ---------------------PLAN----------------------------------

# Create a function that will get starships data from the API
# Create a function that will loop through number of pages from the API collecting starship data from each page
# Create a function that checks if pilots are empty and retrieve the pilot's API call from each starship
# Create a function that takes searches for the corresponding pilot ID from the pilot API call
# Create a function that replaces the pilot API call in the starships to the pilot ID
# Create a function that inserts the starships data with the new information into a new collection

# --------------------IMPORTING LIBRARIES---------------------

import pymongo
import requests
from pprint import pprint

# ---------------------FUNCTIONS---------------------------


def do_api_call(url: str):  # This function completes the API call request by inputting the URL as the arguement
    results = requests.get(url).json()["results"]  # HTTP GET request from the given URL returning the results in a JSON format, storing it in a varibale called "response"
    return results  # returns only the result\'s dictionary from the API call back to the function


def collecting_ship_data():  # This function will loop through the number of pages and collecting the ship data from each page
    url = "https://swapi.dev/api/starships/?page=1"  # this is the URL of the first page
    ship_info = []
    try:
        while requests.get(url).status_code == 200:  # a WHILE loop where condition is that the request is successful
            results = requests.get(url).json()["results"]  # results variable stores
            new_page = requests.get(url).json()["next"]
            url = new_page
            for page in results:
                ship_info.append(page)
    except:
        return ship_info


#pprint(collecting_ship_data())


def do_pilot_call():
    starships = collecting_ship_data()
    for ships in starships:
        for pilots in ships:
            print(ships["pilots"])

do_pilot_call()

