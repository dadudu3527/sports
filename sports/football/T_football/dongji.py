import pymongo

from config import config

client = pymongo.MongoClient(config)
dbname = client['football']
# collection_m = dbname['TeamMatchDetailData']
collection_a = dbname['TeamAdditionalInfo']
set_a = set()
set_b = set()
note = collection_a.find({},{'teamPosition':1})
for i in note:
    teamPosition = i.get('teamPosition')
    if teamPosition:
        for r in teamPosition:
            position = r.get('position')
            detail = r.get('detail')
            set_a.add(position)
            for s in detail:
                ps = s.get('position')
                set_b.add(ps)
            
print(set_a)
print(set_b)
        