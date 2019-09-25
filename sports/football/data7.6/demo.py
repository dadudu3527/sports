import pymongo
client = pymongo.MongoClient('mongodb://root:DRsXT5ZJ6Oi55LPQ@dds-wz90ee1a34f641e41.mongodb.rds.aliyuncs.com:3717,dds-wz90ee1a34f641e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-15344719')
dbname = client['football']
collection = dbname['team']
note = collection.find_one({'teamId':10000})
print(note)