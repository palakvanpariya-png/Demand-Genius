from pymongo import MongoClient

def get_mongo_client(uri="mongodb://localhost:27017", db_name="my_database"):
    client = MongoClient(uri)
    return client[db_name]
