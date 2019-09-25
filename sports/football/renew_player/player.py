import re
import random
import time
import json
import logging
from multiprocessing.pool import ThreadPool

import requests
from lxml import etree
import redis
import pymongo


from config import config
from user_agent import agents

logger = logging.getLogger(__name__)
fh = logging.FileHandler('C:\\sports\\football\\data\\player.log')
sh = logging.StreamHandler()
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class Player:
    def __init__(self):
        self.a = 0
        self.pool = ThreadPool(10)
        client = pymongo.MongoClient(config)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
        self.dbname = client['football']
        self.col = self.dbname['playerErrorId']
        self.collection = self.dbname['player']
        self.collection_t = self.dbname['team']

    def getData(self, url,a):
        shu = a
        headers = {'User-Agent': random.choice(agents)}
        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "H875FZ3594Z343AD"
        proxyPass = "9990096723915515"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {"host": proxyHost, "port": proxyPort,
                                                                    "user": proxyUser, "pass": proxyPass, }

        proxies = {"http": proxyMeta, "https": proxyMeta, }
        try:
            resp = requests.get(url=url, headers=headers, proxies=proxies)
        except:
            shu += 1
            if shu <= 10:
                resp = self.getData(url,shu)
                return resp
            else:
                raise Exception('递归次数超过10')
        if resp.status_code == 200:
            return resp
        else:
            shu += 1
            if shu <= 10:
                resp = self.getData(url,shu)
                return resp
            else:
                raise Exception('递归次数超过10!')

    def get_data(self, url):
        resp = self.getData(url,self.a)
        return resp.text

    def re_parse_data(self,data,player_id):  #赛程赛果数据
        rule = r'var *DATAID=%s,.*?try *\{.*?ABILITY=(\[.*?\]);'%(player_id)
        res = self.is_empty(re.findall(rule,data,re.S))
        if res:
            result = json.loads(res)
        else:
            result = ''
        return result

    def score(self,score_list): #能力值解析
        item = {'shootScore':0,'passScore':0,'tacticsScore':0,'defenceScore':0,'createScore':0}
        if score_list:
            try:
                shootScore = score_list[0][1]
                passScore = score_list[1][1]
                tacticsScore = score_list[2][1]
                defenceScore = score_list[3][1]
                createScore = score_list[4][1]
                item = {
                    'shootScore': shootScore,
                    'passScore': passScore,
                    'tacticsScore': tacticsScore,
                    'defenceScore': defenceScore,
                    'createScore': createScore,
                }
            except Exception:
                pass
        return item
        
    def transfer(self,resp): #转会信息
        response = etree.HTML(resp)
        player_transfer_record = response.xpath('//div[@class="page-one clearfix-row"]/div[3]')
        player_transfer_record_list = []
        if player_transfer_record:
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr')
            for r in range(1,len(tr)):
                item = {}
                player_transfer_date = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[1]/text()'.format(r+1)))
                player_transfer_nature = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[2]/text()'.format(r+1)))
                player_transfer_fee = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[3]/text()'.format(r+1)))
                player_effective_team = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[4]/span/span[2]/text()'.format(r+1)))
                item['date'] = player_transfer_date
                item['type'] = player_transfer_nature
                item['value'] = player_transfer_fee
                item['sourceTeamId'] = player_effective_team
                player_transfer_record_list.append(item)
        return player_transfer_record_list
        
    def player_info(self,resp): #球员资料
        response = etree.HTML(resp)
        player_num = response.xpath('//div[@class="team-header"]/div[2]/p[1]/span[1]/span/text()')
        player_item = {}
        if player_num:
            player_num = int(''.join(player_num).split('#')[-1].strip())
            player_item['number'] = player_num
            
        player_name = response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/p[1]/span/text()')
        player_name = ''.join(player_name).strip()
        if player_name:
            player_item['nameZH'] = player_name
            
        player_info = response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul[@class="info-data"]/li')
        if player_info:
            for i in range(1, len(player_info) + 1):
                player_info_detail_name = self.is_empty(response.xpath(
                    '//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul/li[{}]/span[1]/text()'.format(i)))
                player_info_detail_value = self.is_empty(response.xpath(
                    '//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul/li[{}]/span[2]/text()'.format(i)))
                # player_info_detail_value = ''.join(player_info_detail_value)
                if player_info_detail_value:
                    if player_info_detail_name == '国籍:':
                        player_item['country'] = player_info_detail_value
                    elif player_info_detail_name == '所属球队:':
                        player_item['teamBelongs'] = player_info_detail_value.strip()
                        name = player_info_detail_value.split('(')[0].strip()
                        noteId = self.collection_t.find_one({'name_zh':name},{'teamId':1})
                        if noteId:
                            teamId = noteId.get('teamId')
                            player_item['teamId'] = teamId         
                    elif player_info_detail_name == '合同截止日期:':
                        player_item['contractEndDay'] = player_info_detail_value
                    elif player_info_detail_name == '惯用脚:':
                        player_item['familiarFoot'] = player_info_detail_value
                    elif player_info_detail_name == '出生日期:':
                        player_item['birthday'] = player_info_detail_value
                    elif player_info_detail_name == '位置:':
                        player_item['position'] = player_info_detail_value
                    elif player_info_detail_name == '身高:':
                        player_item['high'] = int(player_info_detail_value.split('c')[0].strip())
                    elif player_info_detail_name == '体重:':
                        player_item['weight'] = int(player_info_detail_value.split('k')[0].strip())
        return player_item
        
    def play_detail(self,resp,team_id,player_id): #球员详情
        response = etree.HTML(resp)
        player_item = {}
        # player_icon = self.parse_url(self.is_empty(response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/@style')))
        player_num = response.xpath('//div[@class="team-header"]/div[2]/p[1]/span[1]/span/text()')
        if player_num:
            player_num = int(''.join(player_num).split('#')[-1])
        else:
            player_num = ''
        player_name = response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/p[1]/span/text()')
        player_name = ''.join(player_name).strip()
        player_name_zh = response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/p[2]/span/text()')
        for p,zm in enumerate(player_name_zh):
            if zm == '\xa0':
                player_name_zh.remove('\xa0')
                player_name_zh.insert(p,' ')
        player_name_zh = ''.join(player_name_zh)
        player_worth = response.xpath('//div[@class="team-header"]/div[@class="worth"]/div[@class="mask"]/span/text()')
        player_worth = ''.join(player_worth).split('：')[-1]
        player_info = response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul[@class="info-data"]/li')
        player_info_detail_url = 'https:'+ self.is_empty(response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/div/a[@class="moer"]/@href'))
        player_five_mango_stars =  self.re_parse_data(resp,player_id)

        player_position_main = response.xpath('//div[@class="lineup-center"]/div[2]/span[2]/text()')
        player_position_m = response.xpath('//div[@class="lineup-center"]/div[2]/span[@class="clearfix-row"]')
        if player_position_m:
            main_li = []
            for main in range(1,len(player_position_main)):
                player_main = response.xpath('//div[@class="lineup-center"]/div[2]/span[{}]/span[2]/text()'.format(main))
                main_li.append(player_main)
            player_position_main = ','.join(main_li)
        else:
            player_position_main = ','.join(player_position_main)
        player_position_less = response.xpath('//div[@class="lineup-center"]/div[4]/span/span[2]/text()')
        player_position_less = ','.join(player_position_less)
        player_goodness = response.xpath('//div[@class="lineup-right"]/div[1]/div[2]/span/text()')
        player_goodness = ' '.join(player_goodness)
        player_weakness = response.xpath('//div[@class="lineup-right"]/div[2]/div[2]/span/text()')
        player_weakness = ' '.join(player_weakness)
        player_transfer_record = response.xpath('//div[@class="page-one clearfix-row"]/div[3]')
        player_transfer_record_list = []
        if player_transfer_record:
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr')
            for r in range(1,len(tr)):
                item = {}
                player_transfer_date = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[1]/text()'.format(r+1)))
                player_transfer_nature = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[2]/text()'.format(r+1)))
                player_transfer_fee = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[3]/text()'.format(r+1)))
                player_effective_team = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[4]/span/span[2]/text()'.format(r+1)))
                item['date'] = player_transfer_date
                item['type'] = player_transfer_nature
                item['value'] = player_transfer_fee
                item['sourceTeamId'] = player_effective_team
                player_transfer_record_list.append(item)
        player_item['playerId'] = player_id
        player_item['teamId'] = team_id
        # player_item['imageLogo'] = player_icon
        player_item['number'] = player_num
        player_item['nameZH'] = player_name
        player_item['nameEN'] = player_name_zh
        name = player_name.split('·')
        if len(name) == 2:
            player_item['nameZHAbbr'] = name[1]
        elif len(name) == 1:
            player_item['nameZHAbbr'] = name[0]
        player_item['worth'] = player_worth
        # player_item['player_info'] = list1
        if player_info:
            for i in range(1, len(player_info) + 1):
                player_info_detail_name = self.is_empty(response.xpath(
                    '//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul/li[{}]/span[1]/text()'.format(i)))
                player_info_detail_value = self.is_empty(response.xpath(
                    '//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul/li[{}]/span[2]/text()'.format(i)))
                # player_info_detail_value = ''.join(player_info_detail_value)
                if player_info_detail_name == '国籍:':
                    player_item['country'] = player_info_detail_value
                elif player_info_detail_name == '惯用脚:':
                    player_item['familiarFoot'] = player_info_detail_value
                elif player_info_detail_name == '出生日期:':
                    player_item['birthday'] = player_info_detail_value
                elif player_info_detail_name == '位置:':
                    player_item['position'] = player_info_detail_value
                elif player_info_detail_name == '身高:':
                    player_item['high'] = int(player_info_detail_value.split('c')[0].strip())
                elif player_info_detail_name == '所属球队:':
                    player_item['teamBelongs'] = player_info_detail_value
                elif player_info_detail_name == '体重:':
                    player_item['weight'] = int(player_info_detail_value.split('k')[0].strip())
                elif player_info_detail_name == '合同截止日期:':
                    player_item['contractEndDay'] = player_info_detail_value
                # list1.append(player_info_detail_value)
        introduction_to_player = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[2]/div[2]/div[1]/text()')
        player_item['score'] = self.score(player_five_mango_stars)
        player_item['firstPosition'] = player_position_main
        player_item['secondPosition'] = player_position_less
        player_item['ability'] = {'player_goodness':player_goodness,'player_weakness':player_weakness}
        player_item['transfer'] = player_transfer_record_list
        player_item['honor'] = self.player_honor(player_info_detail_url)
        player_item['description'] = ','.join(introduction_to_player) #球员简介
        return  player_item

    def player_honor(self,url):
        res = self.get_data(url)
        response = etree.HTML(res)
        tr = response.xpath('//div[@class="trophy-list-panel"]//tr')
        if tr:
            list_honor = []
            for i in range(1,len(tr)+1):
                td = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td'.format(i))
                if td:
                    for r in range(1,len(td)+1):
                        player_honor_item = {}
                        player_honor_title = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[1]/text()'.format(i,r)))
                        player_honor_icon = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/img/@src'.format(i,r)))
                        player_honor_count = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/span/text()'.format(i,r)))
                        player_honor_content_name = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[2]//span[@class="u-name o-hidden float-left text-a-l m-r-5"]/text()'.format(i,r))
                        player_honor_content_time = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[2]//span[@class="float-left"]/text()'.format(i,r))
                        player_honor_content = []
                        for m in zip(player_honor_content_name,player_honor_content_time):
                            con = ' '.join(m)
                            player_honor_content.append(con)
                        player_honor_item['logo'] = player_honor_icon
                        player_honor_item['name'] = player_honor_title
                        player_honor_item['count'] = player_honor_count
                        player_honor_item['detail'] =','.join(player_honor_content)
                        list_honor.append(player_honor_item)
        else:
            list_honor = ''
        return list_honor
        
    def get_interger(self,data):
        try:
            res = int(data)
        except:
            try:
                res = float(data)
            except:
                res = data
        return res

    def is_empty(self, data):  # 处理数据为空的字段 str转int float
        if isinstance(data, list):
            if data != []:
                jieguo = data[0].strip()
                jieguo = self.get_interger(jieguo)
            else:
                jieguo = ''
        else:
            jieguo = self.get_interger(data)
        return jieguo

    def parse_url(self,shuju): #用正则处理球员头像的url
        result = self.is_empty(re.findall(r"background-image: *url\('(.*?)'\);*",shuju))
        result = 'https:' + result
        result = result.split('?')[0]
        if result.split('r/')[-1] == 'default.png':
            result = ''  
        return result

    def save_database(self,player_id,teamid,player): #保存数据库
        try:
            player1 =self.collection.find_one({'playerId':player_id})
            if not player1:
                self.collection.insert_one(player)
            # else:
                # self.collection.update_one({'playerId': player_id},{'$set':player})
        except Exception as e:
            logger.info('{}数据处理异常{}'.format(teamid,e))
            self.col.insert_one({'teamId':teamid,})

    def thread_pool(self,player_url_list,teamid): #team.py模块调用
            for url in player_url_list:
                self.renew_errorid(url,teamid)
                # self.run_player(url,teamid)
               #self.pool.apply_async(self.run_player,args=(url,teamid))
            #self.pool.close()
            #self.pool.join()
    
    def renew_errorid(self,url,teamId): #teamId错误的球员
        player_id = int(url.split('-')[-1])
        note = self.collection.find_one({'playerId':player_id},{'teamId':1,'teamBelongs':1})
        tid = note.get('teamId')
        if teamId != tid:
            name = note.get('teamBelongs','')
            if name:
                name = name.split('(')[0]
                noteId = self.collection_t.find_one({'name_zh':name},{'teamId':1})
            else:
                noteId = ''
                
            if noteId:
                tmId = noteId.get('teamId')
                if tmId == teamId:
                    self.collection.update_one({'playerId': player_id}, {'$set': {'teamId': teamId}})
                else:
                    self.collection.update_one({'playerId': player_id}, {'$set': {'teamId': tmId}})
            else:
                self.collection.update_one({'playerId': player_id}, {'$set': {'teamId': teamId}})
                
    def renew_id(self): #更新球员teamId
        note = self.collection.find({},{'playerId':1,'teamBelongs':1})
        for i in note:
            tid = i.get('playerId')
            name = i.get('teamBelongs','')
            if name:
                name = name.split('(')[0].strip()
                noteId = self.collection_t.find_one({'name_zh':name},{'teamId':1})
                if noteId:
                    tmId = noteId.get('teamId')
                    self.collection.update_one({'playerId': tid}, {'$set': {'teamId': tmId}})
                    print('playerId:{}的球队已更新'.format(tid))
        
    def suo(self): #剔除国家队
        note = self.collection_t.find({'leagueId':2773},{'teamId':1})
        for i in note:
            teamid = i.get('teamId')
            note_player = self.collection.find({'teamId':teamid})
            for r in note_player:
                name = r.get('teamBelongs','')
                playerId = r.get('playerId')
                if name:
                    name = name.split('(')[0]
                    teamId = self.collection_t.find_one({'name_zh':name},{'teamId':1})
                    if teamId:
                        teamId = teamId.get('teamId')
                    else:
                        teamId = ''
                else:
                    teamId = ''
                self.collection.update_one({'playerId': playerId}, {'$set': {'teamId': teamId}})
                
    def country(self): #国家队
        note = self.collection_t.find({'leagueId':2773},{'teamId':1})
        for i in note:
            teamid = i.get('teamId')
            # self.mdb.sadd('countryTeamId',player_id)
            print(teamid)
            
    def player_ur(self, data):  # 球员url
        response = etree.HTML(data)
        resp = response.xpath('//div[@class="page-one clearfix-row"]/div[2]')
        player_name = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[1]/div[1]/text()'))
        player_url = []
        if resp and player_name == '球队阵容':
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr')
            for r in range(2, len(tr) + 1):
                url = self.is_empty(response.xpath(
                    '//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if not url:
                    url = self.is_empty(response.xpath(
                        '//div[@class="page-one clearfix-row"]/div[1]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if url:
                    qiuyuan_detail = self.get_interger(url.split('-')[-1])
                    print('xinxin--{}'.format(qiuyuan_detail))
                    player_url.append(qiuyuan_detail)
        return player_url
            
    def renew_league(self,team_id): #更新球队所属联赛
        try:
            url = 'https://data.leisu.com/team-{}'.format(team_id)
            resp = self.get_data(url)
            response = etree.HTML(resp) 
            team_info = self.is_empty(response.xpath('//div[@class="clearfix-row bd-box info"]/ul[@class="info-data"]/li[1]/span[1]/text()'))
            if team_info != '所属联赛:':
                players = self.player_ur(resp)
                for i in players:
                    n_player = self.collection.find_one({'playerId':i})
                    if n_player:
                        self.collection.update_one({'playerId':i},{'$set':{'countryTeamId':team_id}})
        except Exception as e:
            self.mdb.sadd('teamcountry',team_id)
            logger.info('{}：此球队更新所属联赛失败，已加入缓存！{}'.format(team_id,e))

    def run_player(self,url,teamid): #添加遗漏球员
        player_id = int(url.split('-')[-1]) 
        resp = self.get_data(url)
        player_data = self.play_detail(resp,teamid,player_id)
        self.save_database(player_id,teamid,player_data)
        logger.info('playerId为{}选手添加成功！'.format(player_id))
        # print(player_data)
        
    def pool_run(self,items):
        player_id = items.get('playerId')
        self.renew_player(player_id)
        
    def get_players(self): #数据库获取所有球员
        li = []
        note = self.collection.find({},{'playerId':1})
        for r in note:
            player_id = r.get('playerId')
            li.append(player_id)
        for i in li:
            # self.pool_run(i)
            self.pool.apply_async(self.renew_player,args=(i,)) 
        self.pool.close()
        self.pool.join()
        
    def genxing(self,resp,playerId,player_item): #更新球员能力五芒星
        # note = self.collection.find_one({'playerId':playerId},{'playerId':1,'score':1})
        # if note:
            # score = note.get('score')
            # dic = {"shootScore": 0, "passScore": 0, "tacticsScore": 0, "defenceScore": 0, "createScore": 0}
            # if score == dic or not score:
        player_five_mango_stars =  self.re_parse_data(resp,playerId)
        score_lei = self.score(player_five_mango_stars)
        if score_lei.get('shootScore') != 0 and (score_lei.get('passScore') != 0 and score_lei.get('tacticsScore')):
            player_item['score'] = score_lei
                # print('{}：score有值！'.format(player_id))
        return player_item
        
    def genxing_2(self): #更新球员能力五芒星2
        note = self.collection.find({},{'playerId':1,'score':1})
        for i in note:
            score = i.get('score')
            playerId = i.get('score')
            dic = {"shootScore": 0, "passScore": 0, "tacticsScore": 0, "defenceScore": 0, "createScore": 0}
            if score == dic or not score:
                url = 'https://data.leisu.com/player-{}'.format(playerId)
                resp = resp = self.get_data(url)
                player_five_mango_stars =  self.re_parse_data(resp,playerId)
                score_lei = self.score(player_five_mango_stars)
                if score_lei != dic:
                    player_item={'score':score_lei}
                    self.collection.update_one({'playerId': player_id},{'$set':player_item})
                    print('{}：score有值！'.format(player_id))
            else:
                print('{}:score存在！'.format(playerId))
    
    def zhougen(self): #球员能力值周更
        note = self.collection.find({},{'playerId':1})
        for i in note:
            player_id = i.get('playerId')
            url = 'https://data.leisu.com/player-{}'.format(player_id)
            try:
                resp = self.get_data(url)
                player_five_mango_stars =  self.re_parse_data(resp,player_id)
                player_score = self.score(player_five_mango_stars)
                self.collection.update_one({'playerId': player_id},{'$set':{'score':player_score}})
            except Exception as e:
                self.mdb.sadd('errorPlayerId',player_id)
                logger.info('score-playerId:{} 下载失败--->{}'.format(player_id,e))
                
    def renew_player(self,player_id): #更新球员国籍，能力值，转会信息，teamId
        url = 'https://data.leisu.com/player-{}'.format(player_id)
        player_item = {}
        try:
            resp = self.get_data(url)
            player_item = self.player_info(resp)
            # player_five_mango_stars =  self.re_parse_data(resp,player_id)
            # player_item['score'] = self.score(player_five_mango_stars)
            player_item = self.genxing(resp,player_id,player_item)
            transfer = self.transfer(resp)
            if transfer:
                player_item['transfer'] = transfer
            if player_item:
                self.collection.update_one({'playerId': player_id},{'$set':player_item})
            # dic = {"score": {"shootScore": 0, "passScore": 0, "tacticsScore": 0, "defenceScore": 0, "createScore": 0}, "transfer": []}
            # transfer = []
            # if not player_item['transfer']:
                # self.mdb.sadd('nullPlayerId',player_id)
                print(str(player_id) +':'+ json.dumps(player_item,ensure_ascii=False))
        except Exception as e:
            self.mdb.sadd('errorPlayerId',player_id)
            logger.info('playerId:{} 下载失败--->{}'.format(player_id,e))
        return player_item
        
    def redis_read(self):
        while True:
            player_id = self.mdb.spop('errorPlayerId')
            if player_id:
                self.renew_player(player_id)
            else:
                break
            
if __name__ == '__main__':
    a = Player()
    a.get_players()
    print('开始更新redis')
    a.redis_read()
    print('更新完成！')
    
    # a.renew_id() #更新球员的teamId
    # a.renew_player(13435)
  
       
    
    