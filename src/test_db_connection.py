from pymongo import MongoClient

client = MongoClient("mongodb://devuser:devpass@mongo:27017/pipeline_db")
print(client.server_info())
