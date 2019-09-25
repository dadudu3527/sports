import pymongo
import datetime
from threading import Thread
config = 'mongodb://root:DRsXT5ZJ6Oi55LPQ@dds-wz90ee1a34f641e41.mongodb.rds.aliyuncs.com:3717,dds-wz90ee1a34f641e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-15344719'

client = pymongo.MongoClient(config)
dbnameFootball = client['football']
# collection_league = dbnameFootball['league']  # 读取level所创建集合
collection_2 = dbnameFootball['seniorSchedule']  # 高级赛程集合
collection_3 = dbnameFootball['generalSchedule']  # 普通赛程集合
# collection_4 = dbnameFootball['seniorMatchResult']  # 高级赛果集合
# collection_5 = dbnameFootball['generalMatchResult']  # 普通赛果集合



def senior():
    now = datetime.datetime.now()
    before = now - datetime.timedelta(minutes=300)
    timestamp = before.timestamp()
    # s_note = collection_2.find({'matchTime': {'$lt': round(timestamp)}})
    # for i in s_note:
        # matchId = i.get('matchId')
        # print(matchId)
    collection_2.update_many({'matchTime':{'$lt':round(timestamp)}},{'$set':{'delete':1}})
    # collection_2.update_many({'matchTime':{'$gt':round(timestamp)}},{'$set':{'delete':0}})

    # notes = collection_2.find({},{'matchId':1,'matchTime':1})
    # for i in notes:
        # matchId = i.get('matchId')
        # matchTime = i.get('matchTime')
        # if isinstance(matchTime,str):
            # collection_2.remove({'matchId':matchId})

def general():
    now = datetime.datetime.now()
    before = now - datetime.timedelta(minutes=300)
    timestamp = before.timestamp()
    # g_note = collection_3.find({'matchTime': {'$lt': round(timestamp)}},{'matchId':1})
    # for i in g_note:
        # matchId = i.get('matchId')
        # print(matchId)
    
    collection_3.update_many({'matchTime': {'$lt': round(timestamp)}}, {'$set': {'delete': 1}})
    # collection_3.update_many({'matchTime':{'$gt':round(timestamp)}},{'$set':{'delete':0}})

    # notes = collection_3.find({}, {'matchId': 1, 'matchTime': 1})
    # for i in notes:
        # matchId = i.get('matchId')
        # matchTime = i.get('matchTime')
        # if isinstance(matchTime, str):
            # collection_3.remove({'matchId': matchId})

senior = Thread(target=senior)
general = Thread(target=general)
senior.start()
general.start()
senior.join()
general.join()
print('更新结束！')