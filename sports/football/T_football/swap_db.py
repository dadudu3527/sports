import time
import pymongo
import json
import random

from config import config

client = pymongo.MongoClient(config)
dbnameFootball = client['football']
collection_player = dbnameFootball['player']
# collection = dbnameFootball['playerCharacteristic']
# notes = collection.find({},{'_id':0})
setType = set()
# item = {
    # '传球':'0',
    # '直塞球':'1',
    # '关键传球':'2',
    # '盘带':'3',
    # '终结能力':'4',
    # '远射':'5',
    # '直接任意球':'6',
    # '二过一':'7',
    # '防守贡献':'8',
    # '反越位意识':'9',
    # '纪律性':'10',
    # '争抢高空球':'11',
    # '抢断':'12',
    # '注意力集中':'13',
    # '传中':'14',
    # '头球能力':'15',
    # '扑救近距离射门':'16',
    # '扑救远射':'17',
    # '擅长扑点球':'18',
    # '反应速度':'19',
# }
# dic_level = {'非常强':random.choice(['7','8']),'强':'6','非常弱':'2','弱':'3'}


# for note in notes:  #匹配没有playerId的球员
    # playerId = note.get('playerId')
    # if not playerId:
        # teamId = note.get('teamId')
        # playerName_en = note.get('playerName_en')
        # if teamId and playerName_en:
            # playerName_en = playerName_en.strip()
            # Name_en = playerName_en.split(' ')[-1]
            # player_note = collection_player.find_one({'teamId':teamId,'nameEN':{'$regex':Name_en}},{'playerId':1})
            # if player_note:
                # playerId = player_note.get('playerId')
                # collection.update_one({'teamId':teamId,'playerName_en':playerName_en},{'$set':{'playerId':playerId}})
                # print('{}:添加playerId成功！'.format(playerId))
            # else:
                # Name_en = playerName_en.split(' ')[0]
                # player_note = collection_player.find_one({'teamId':teamId,'nameEN':{'$regex':Name_en}},{'playerId':1})
                # if player_note:
                    # playerId = player_note.get('playerId')
                    # collection.update_one({'teamId':teamId,'playerName_en':playerName_en},{'$set':{'playerId':playerId}})
                    # print('{}:添加playerId成功.'.format(playerId))
# a = 0
# for note in notes: #添加球员能力值
    # playerId = note.get('playerId')
    # if playerId:
        # strengths = note.get('strengths')
        # weaknesses = note.get('weaknesses')
        # playStype = note.get('playStype')
        # level = note.get('level','-')
        # collection_player.update_one({'playerId':playerId},{'$set':{'level':level}})
        # player_item_s = {}
        # player_item_w = {}
        # player_playStype = []
        # for p,s in enumerate(strengths):
            # if p<=8:
                # s_type = s[1]
                # s_level = s[2]
                # s_key = item.get(s_type)
                # if s_key:
                    # player_item_s[s_key] = dic_level.get(s_level) 
            # else:
                # break
                
        # for w in weaknesses:        
            # w_type = w[1]
            # w_level = w[2]
            # w_key = item.get(w_type)
            # if w_key:
                # player_item_w[w_key] = dic_level.get(w_level)
                
        # for p in playStype:
            # p_text = p[1]
            # player_playStype.append(p_text)
        # sw_items = {}
        # if player_item_s:
            # sw_items['Advantages'] = player_item_s
        # if player_item_w:
            # sw_items['Disadvantages'] = player_item_w
        # if player_playStype:
            # sw_items['Game_style'] = '，'.join(player_playStype)+'。'
            # # 'Advantages':player_item_s,
            # # 'Disadvantages':player_item_w,
            # # # 'Game_style':'，'.join(player_playStype)+'。'
        
        # try:
            # player = collection_player.find_one({'playerId':playerId},{'playerId':1,'Advantages':1,'Disadvantages':1,'Game_style':1})
            # Advantages = player.get('Advantages')
            # Disadvantages = player.get('Disadvantages')
            # Game_style = player.get('Game_style')
            # if Game_style == '。':
                # a += 1
                # collection_player.update_one({'playerId':playerId},{'$set':{'Game_style':''}})
                # print('{}:更改成功！'.format(playerId))
            # # if not Advantages and not Disadvantages:
                # # a += 1
                # # collection_player.update_one({'playerId':playerId},{'$set':sw_items})
                # # print('{}:添加能力强弱成功！'.format(playerId))
        # except:
            # with open('C:\\sports\\football\\data\\sw_ability.json','a',encoding='utf-8') as f:
                # f.write(str(playerId)+',')
            
# print('共添加成功%d个球员能力'%a)            
se = set()
notes = collection_player.find({},{'_id':0})  #28277 
for note in notes:
    playerId = note.get('playerId')
    firstPosition = note.get('firstPosition')
    secondPosition = note.get('secondPosition')
    
    firstPosition_li = firstPosition.split(',')
    secondPosition_li = secondPosition.split(',')
    feild = '左前位'
    
    if firstPosition:
        for i in firstPosition_li:
            if i == feild:
                index_i = firstPosition_li.index(i)
                firstPosition_li.pop(index_i)
                firstPosition_li.insert(index_i,'左前卫')
                print(playerId)
                
    if secondPosition:
        for i in secondPosition_li:
            if i == feild:
                index_i = secondPosition_li.index(i)
                secondPosition_li.pop(index_i)
                secondPosition_li.insert(index_i,'左前卫')
                print(playerId)
    firstPo = ','.join(firstPosition_li)   
    secondPo = ','.join(secondPosition_li)
    print(firstPo,secondPo)
    collection_player.update_one({'playerId':playerId},{'$set':{'firstPosition':firstPo,'secondPosition':secondPo}})
    # if firstPosition =='DM,防守型中场,DR,右后卫' or secondPosition == 'DM,防守型中场,DR,右后卫' :
        # print(note.get('playerId'))
        
    # se.add(firstPosition)
    # se.add(secondPosition)
    # s_type = firstPosition.split(',')
    # w_type = secondPosition.split(',')
    
    # if isinstance(s_type,str):
        # setType.add(s_type)
    # elif isinstance(s_type,list):
        # s_type = set(list(s_type))
        # setType.update(s_type)
        
    # if isinstance(w_type,str):
        # setType.add(w_type)
    # elif isinstance(w_type,list):
        # w_type = set(list(w_type))
        # setType.update(w_type)
# print(setType)        
# print(se)        
        
        
        
        
        
        