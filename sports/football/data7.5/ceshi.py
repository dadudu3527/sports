import pymongo

client = pymongo.MongoClient()
dbname = client['test']
collection = dbname['team']
note = collection.find_one({'teamId':10000})
print(note)