import time
import pymongo
import json
import random

from config import config

client = pymongo.MongoClient(config)
dbnameFootball = client['football']
collection_player = dbnameFootball['player']
collection_scoreBoard = dbnameFootball['scoreBoard']
note = collection_scoreBoard.find_one({'seasonId':8457,'round':'总积分'},{'tableDetail':1})
tableDetail = note.get('tableDetail')
li = []
for p,i in enumerate(tableDetail):
    # i['rank'] = p+1
    t = i.get('teamId')
    if t != 10068:
        li.append(i)
collection_scoreBoard.update_one({'seasonId':8457,'round':'总积分'},{'$set':{'tableDetail':li}})    
# a = 0
# notes = collection_player.find({},{'playerId':1,'nameZH':1})
# for i in notes:
    # playerId = i.get('playerId')
    # playerName = i.get('nameZH','美杜莎女王')
    # if playerName != '美杜莎女王':
        # if '-' in playerName:
            # playerName = playerName.strip().replace('_','·')
            # collection_player.update_one({'playerId':playerId},{'$set':{'nameZH':playerName}})
            # a += 1
            # print('{}:修改成功！'.format(playerId))
# print(a)