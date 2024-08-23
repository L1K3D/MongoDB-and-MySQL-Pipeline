from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import pandas as pd

uri = "your_mong_db_atlas_uri"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    
db = client["db_produtos"]
collection = db["produtos"]

collection.update_many({}, {"$rename": {"lat": "Latitude", "lon": "Longitude"}})

query = {"Categoria do Produto": "livros"}

lista_livros = []

for doc in collection.find(query):
    
    lista_livros.append(doc)
    
df_livros = pd.DataFrame(lista_livros)

df_livros["Data da Compra"] = pd.to_datetime(df_livros["Data da Compra"], format="%d/%m/%Y")

df_livros["Data da Compra"] = df_livros["Data da Compra"].dt.strftime("%Y-%m-%d")

df_livros.to_csv("../data/tabela_livros.csv", index=False)