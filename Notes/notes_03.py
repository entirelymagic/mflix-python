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

"""
    Upserts vs. Updates
    Sometimes, we want to update a document, but we're not sure if it exists in the collection.
    
    We can use an "upsert" to update a document if it exists, and insert it if it does not exist.
    
    In the following example, we're not sure if this video game exists in our collection, but we want to make sure there 
is a document in the collection that contains the correct data. 
    
    This operation may do one of two things:
    
    If the predicate matches a document, update the document to contain the correct data.
    
    If the document doesn't exist, create the desired document.
"""

# -----------------------------------------------------

"""Unlike delete_one, delete_many deletes all documents that match the supplied predicate. Because of this behavior, 
delete_many is a little more "dangerous". 

    - delete_one will delete the first document that matches the supplied predicate. 
    - delete_many will delete all documents matching the supplied predicate. 
    - The number of documents deleted can be accessed via the deleted_count property on the DeleteResult object
     returned from a delete operation. 
"""
