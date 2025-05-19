import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

MONGO_URI     = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
if not MONGO_URI or not MONGO_DB_NAME:
    raise RuntimeError("MONGO_URI ou MONGO_DB_NAME não definido em .env")

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB_NAME]

# Agora criamos índices em PRODUCAO, PROCESSAMENTO e COMERCIALIZACAO
collections = {
    "producao":      [("categoria", 1), ("tipo_produto", 1)],
    "processamento": [("categoria", 1), ("tipo_produto", 1)],
    "comercializacao":[("categoria", 1), ("tipo_produto", 1)],
    "importacao":    [("pais", 1), ("quantidade_kg", 1), ("valor_usd", 1)],
    "exportacao":    [("pais", 1), ("quantidade_kg", 1), ("valor_usd", 1)]
}

for coll_name, keys in collections.items():
    db[coll_name].create_index(keys, unique=True)