"""Import pymongo client"""

from pymongo import MongoClient

# set uri connection to a variable
uri = "mongodb+srv://m220student:m220password@mflix.rdhe6.mongodb.net/test"  # contain username and password, as well as the database name we want to connect by default.

client = MongoClient(uri,connectTimeoutMS=200, retryWrites=True)  # pass the uri string to the client object to instantiate it
"""Here can be defined other configuration options like: how the driver connect to mongodb
and the nature of the operations that are  performed using that connection """

# client.stats  # see the stats of the connections
# Database(MongoClient(host=['<mongo uri>:27017'], document_class=dict, tz_aware=False, connect=True), 'stats')

client.list_database_names()  # list the database names
mflix = client.mflix   # (create a client database for mflix
# mflix = client["mflix"]  # Identical with the one from top using dictionary acceser instead of object acceser
mflix.list_collection_names() # list collections names  from mflix database object

movies = mflix.movies
x = movies.count_documents({})  # {} represents the pipeline of the documents we get back
print(x)