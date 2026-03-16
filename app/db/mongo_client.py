from pymongo import MongoClient


client = MongoClient(f'mongodb://mongo:27017')

db = client['digital_hunter']

targets = db['targets']
attacks = db['attacks']