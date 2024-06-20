from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging

logger = logging.getLogger('logger')

# Replace the placeholder with your Atlas connection string
uri = "mongodb://localhost:27017"


# Send a ping to confirm a successful connection
def store(db_name, collection_name, record):
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db[collection_name]
        record_id = collection.insert_one(record).inserted_id
    except Exception as ex:
        logger.info("Error storing the record: " + str(ex) + "...")
        record_id = None
    return record_id


def retrieve_record(db_name, collection_name, query):
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db[collection_name]
        record = collection.find_one(query)
    except Exception as ex:
        logger.info("Error retrieving the record: " + str(ex) + "...")
        record = None
    return record


def update_record(db_name, collection_name, query, update):
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db[collection_name]
        record_id = collection.update_one(query, update)
    except Exception as ex:
        logger.info("Error updating the record: " + str(ex) + "...")
        record_id = None
    return record_id
