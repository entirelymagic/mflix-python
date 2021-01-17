import pymongo

from bson.json_util import dumps


uri = uri = "mongodb+srv://m220student:m220password@mflix.rdhe6.mongodb.net/test"
client = pymongo.MongoClient(uri)

print(client.list_database_names())
mflix = client.sample_mflix
# ['sample_airbnb', 'sample_analytics', 'sample_geospatial', 'sample_mflix', 'sample_restaurants',
# 'sample_supplies', 'sample_training', 'sample_weatherdata', 'admin', 'local']

movies = mflix.movies

# one random movie

one_random_movie = movies.find_one() # find_one do not return a cursor

# print all movie objects that include Salma Hayek in cast and print it
# cursor = movies.find({"cast": "Salma Hayek"})
# print(dumps(cursor, indent=2))

# filter movies after cast , show title and do not show _id
# cursor = movies.find({"cast": "Salma Hayek"}, {"title": 1, "_id": 0})
# print(dumps(cursor, indent=2))

cursor =movies.aggregate([
    {
        '$match': {
            'countries': {
                '$ne': "Null"
            }
        }
    }, {
        '$project': {
            'title': 1,
            '_id': 1
        }
    }
])
print(list(cursor))
