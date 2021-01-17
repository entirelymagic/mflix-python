import pymongo

from bson.json_util import dumps


uri = "mongodb+srv://m220student:m220password@mflix.rdhe6.mongodb.net/test"
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

pipeline = [
    {'$match': {'directors': "Sam Raimi"}},
    {'$project': {'title': 1, '_id': 0, 'cast': 1}},
    {'$count': 'num_movies'}
]
sorted_aggregation = movies.aggregate(pipeline)
print(dumps(sorted_aggregation, indent=2))


match = {'directors': "Sam Raimi"}
project = {'title': 1, '_id': 0, 'cast': 1}
skipped_cursor = movies.find(match, project).skip(12)
print(dumps(skipped_cursor, indent=2))


# Using Find method
match = {'directors': "Sam Raimi"}
project = {'title': 1, '_id': 0, 'cast': 1, 'year': 1}
skipped_sorted_cursor = movies.find(match, project).sort('year', 1).skip(10)
print(dumps(skipped_sorted_cursor, indent=2))


# Using pipeline
pipeline = [
    {'$match': {'directors': 'Sam Raimi'}},
    {'$project': {'_id': 0, 'year': 1, 'title': 1, 'cast': 1}},
    {'$sort': {'year': 1}},
    {'$skip': 10}
]
sorted_skipped_aggregation = movies.aggregate(pipeline)
print(dumps(sorted_skipped_aggregation, indent=2))


