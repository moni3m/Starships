# ---------------------------------TASK--------------------------------------------------------------

# The data in this database has been pulled from https://swapi.dev/. As well as 'people', the API has data on starships.
# In Python, pull data on all available starships from the API. The "pilots" key contains URLs pointing to the characters who pilot the starship.
# Use these to replace 'pilots' with a list of ObjectIDs from our characters collection,
# then insert the starships into their own collection.
# Use functions at the very least!

# ---------------------------------PLAN------------------------------------------------------

# Create a function that will get starships data from the API
# Create a function that will loop through number of pages from the API collecting starship data from each page
# Create a function that checks if pilots are empty and retrieve the pilot's API call from each ship
# Create a function that takes searches for the corresponding pilot ID from the pilot API call
# Create a function that replaces the pilot API call in the starships to the pilot ID
# Create a function to drop a collection if it already exists
# Create a function to create a collection
# Create a function that inserts the starships data with the new information into a new collection

# ---------------------------IMPORTING LIBRARIES--------------------------------------------------

import pymongo
import requests
from pprint import pprint

# -------------------------------FUNCTIONS--------------------------------------------------------


def drop_collection(collection_name: str, database_name: str):  # Function will drop a collection if it already exists
    client = pymongo.MongoClient("localhost:27017")  # Creating a pymongo client
    db = client[database_name]  # Creating a database with the inputted database name
    try:
        print("Collection Dropped")
        return db.drop_collection(collection_name)  # this will drop the collection
    except:
        return print(f"{collection_name} collection could not be dropped.")


# EXTRACT
def do_api_call(url: str):  # This function completes the API call request by inputting the URL as the argument
    return requests.get(url).json()  # HTTP GET request from the given URL returning the results in a JSON format (dict)


def collecting_ship_data():  # Function will loop through any number of pages, collecting the ship data from each page
    url = "https://swapi.dev/api/starships/?page=1"  # URL stores the url of the first page
    ship_info = []  # a list to store the ship data
    try:
        while requests.get(url).status_code == 200:  # a WHILE loop where condition is that the request is successful
            response = do_api_call(url)["results"]  # response variable stores the list from 'results' from our call
            new_page = do_api_call(url)["next"]  # this variable stores the endpoint of our next page
            url = new_page  # this replaces the old url with the url of the 'new_page'
            for page in response:  # FOR loop iterates over every page in the response variable
                ship_info.append(page)  # the data from each page is added to the list 'ship_info'
    except:
        return ship_info  # list 'ship_info' is returned to the function to be used later


def connect_to_database(database_name: str): # Function to connect to the given database to be used later for queries
    client = pymongo.MongoClient("localhost:27017")  # Creating a pymongo client
    db = client[database_name]  # Initalising a database
    return db  # returns the established database back to the function


# TRANSFORM
def pilot_replacement():  # Function to loop through pilot APIs and match it to the corresponding pilot ID to replace it
    db = connect_to_database("starwars")  # Uses previous function to establish a connection to MongoDB
    starships = collecting_ship_data()  # Uses previous function to access the list of ships assigning it to 'starships'
    for ships in starships:  # FOR loop to iterate over each ship in the list of ships
        if ships["pilots"]:  # IF statement checks whether each ship has pilots, ignoring the empty list
            for pilot in ships["pilots"]:  # FOR loop iterates through the list of pilots that are not empty
                pilot_data = requests.get(pilot).json()  # GET request call for each pilot APIs, returning a dictionary
                pilot_name = pilot_data["name"]  # access the 'name' key from pilot_data dict, returning the value of name
                pilot_id = db.characters.find_one({"name": pilot_name}, {"_id" : 1})  # MongoDB query to retrieve the id of the pilot using the pilots name
                ships["pilots"][ships["pilots"].index(pilot)] = pilot_id  # replaces API with ID using the index of each pilot api
    return starships  # returns newly transformed starship data back to the function to be used later


# LOAD
def collection_creation(collection_name: str, database_name: str):  # Function to create a collection, using 'connect_to_collection' func
    db = connect_to_database(database_name)  # connects to the given database
    try:  # TRY CATCH block will try to create the collection, printing that it either has been created if succesful
        print("Collection Creation Successful")
        return db.create_collection(collection_name)  # creates the collection 'starships' and returns it to th function
    except:  # will print that collection could not be created if code fails to create a collection
        print("Collection Creation Failed")


def insert_into_collection(collection_name: str, database_name: str):  # function that will insert data into collection
    connect_to_database(database_name)  # using previous function, it establishes a connection to the given database
    try:  # T.C BLOCK will try to insert data into collection, printing that it has been successfully created
        starships_collection = collection_creation(collection_name, database_name)  # using a previous function, it creates a collection
        starships_collection.insert_many(pilot_replacement())  # inserts the data stored in a previous function to insert data into the collection
        print("Insertion Successful")
    except:  # If function fails to insert the data into collection, it will print 'Failed'
        print("Insertion Failed")
    return


drop_collection("starships", "starwars")
insert_into_collection("starships", "starwars")