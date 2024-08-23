from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

uri = "your_connection_uri"

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

response = requests.get("https://labdados.com/produtos")

docs = collection.insert_many(response.json())

collection.count_documents({})