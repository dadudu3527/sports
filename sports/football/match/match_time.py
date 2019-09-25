import pymongo
import datetime
from threading import Thread
from config import config

client = pymongo.MongoClient(config)
dbnameFootball = client['football']
collection_sb = dbnameFootball['scoreBoard']  # 积分榜
collection_2 = dbnameFootball['seniorSchedule']  # 高级赛程集合
collection_3 = dbnameFootball['generalSchedule']  # 普通赛程集合
collection_4 = dbnameFootball['seniorMatchResult']  # 高级赛果集合
collection_5 = dbnameFootball['generalMatchResult']  # 普通赛果集合

def scoreBoard(): #积分榜去重
    seasonId_li = []
    notes = collection_sb.find({},{'seasonId':1,'round':1})
    for note in notes:
        seasonId = note.get('seasonId')
        seasonId_li.append(seasonId)
    seasonId_li = set(seasonId_li)
    for i in seasonId_li:
        jilv = collection_sb.find({'seasonId':i,'round':'总积分'},{'_id':1})
        for d,r in enumerate(jilv):
            if d != 0:
                id = r.get('_id')
                collection_sb.remove({'_id':id})
                print('season:{}总积分去重成功！'.format(i))

        jilv = collection_sb.find({'seasonId': i},{'round':1})
        length = len(list(jilv))
        for le in range(1,length):
            round = collection_sb.find({'seasonId':i,'round':le},{'_id':1})
            for d, r in enumerate(round):
                if d != 0:
                    id = r.get('_id')
                    collection_sb.remove({'_id': id})
                    print('season:{} round{}去重成功！'.format(i,le))
                        
def matchResult(collection):
    note = collection.find({},{'matchId':1,'seasonYear':1,'matchTime':1})
    for i in note:
        time1 = i.get('matchTime')
        if isinstance(time1,str):
            matchId = i.get('matchId')
            year = i.get('seasonYear')
            try:
                year_int = int(year)
                time_str = '{}-{}'.format(year_int,time1)
                # a = '2018-01-28 20:00'
                time_sp = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M')
                timestamp = round(time_sp.timestamp())
                collection.update_one({'matchId':matchId},{'$set':{'matchTime':timestamp}})
            except:
                month = int(time1.split('-')[0])
                year_li = year.split('-')
                year_min = int(year_li[0].strip())
                year_max = int(year_li[1].strip())
                if month > 6 and month <= 12:
                    time_str = '{}-{}'.format(year_min, time1)
                else:
                    time_str = '{}-{}'.format(year_max, time1)
                time_sp = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M')
                timestamp = round(time_sp.timestamp())
                collection.update_one({'matchId': matchId}, {'$set': {'matchTime': timestamp}})
            print('{}:时间戳更改成功！'.format(matchId))
            

          
def manageMatch(collection,collection_m):
    notes = collection.find({"statusId": 11},{'_id':0,'delete':0})
    for note in notes:
        matchId = note.get('matchId')
        collection.update_one({'matchId':matchId},{'$set':{'delete':1}})
        collection_m.insert_one(note)
        

# scoreBoard()   
    
# manageMatch(collection_2,collection_4)
# manageMatch(collection_3,collection_5)
                
matchResult(collection_4)
matchResult(collection_5)