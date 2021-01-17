import pymongo

from bson.json_util import dumps


uri = "mongodb+srv://m220student:m220password@mflix.rdhe6.mongodb.net/test"
client = pymongo.MongoClient(uri)
mflix = client.sample_mflix
movies = mflix.movies


pipeline = [
    {'$match': {'directors': 'Sam Raimi'}},
    {'$project': {'_id': 0, 'title': 1, 'imdb.rating': 1}},
    {'$group': {'_id': 0, 'average_rating': {'$avg': '$imdb.rating'}}},
]

aggregation = movies.aggregate(pipeline)
print(dumps(aggregation, indent=2))

