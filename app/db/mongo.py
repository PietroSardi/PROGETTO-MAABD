import os
from pymongo import MongoClient

URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
DB = os.environ.get("MONGO_DB", "findetect")

_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(URI)
    return _client[DB]
