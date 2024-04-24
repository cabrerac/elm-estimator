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
        logger.info("Error storing the task: " + str(ex) + "...")
        record_id = None
    return record_id
