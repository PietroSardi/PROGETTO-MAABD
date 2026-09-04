# Connessione a MongoDB. Un solo MongoClient per processo, creato alla prima
# chiamata: il client gestisce internamente un pool di connessioni, crearne uno
# per richiesta sarebbe uno spreco. Il resto dell'app chiama get_db() e non sa
# nulla di URI o nome del database (cambiando questo file si cambia tutto).
import os
from pymongo import MongoClient

# ?replicaSet=rs0: il driver parla col replica set (scoperta del primary), non
# con un server singolo. Nome DB e URI sovrascrivibili da variabili d'ambiente.
URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
DB = os.environ.get("MONGO_DB", "findetect")

_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(URI)
    return _client[DB]
