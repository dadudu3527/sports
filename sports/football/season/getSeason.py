import re
import time
import json
import logging
import random

from lxml import etree
import redis
import pymongo
import requests

from config import config
from user_agent import agents

logger = logging.getLogger(__name__)
fh = logging.FileHandler('C:\\sports\\football\\data\\getSeason.log')
# sh = logging.StreamHandler()
logger.addHandler(fh)
# logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class NewScore:
    def __init__(self):
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True,password='fanfubao')
        client = pymongo.MongoClient(config)
        dbnameFootball = client['football']
        self.col_area = dbnameFootball['area']
        self.col_country = dbnameFootball['country']
        self.col_season = dbnameFootball['season']
        self.col_league = dbnameFootball['league']
        
        self.col_renew_season = dbnameFootball['season_w']
        self.a = 0

    def getData(self, url, a):
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
            resp = requests.get(url=url, headers=headers, proxies=proxies,timeout=5)
        except:
            shu += 1
            if shu <= 10:
                resp = self.getData(url, shu)
                return resp
            else:
                raise Exception('递归次数超过10')
        if resp.status_code == 200:
            return resp
        else:
            shu += 1
            if shu <= 10:
                resp = self.getData(url, shu)
                return resp
            else:
                raise Exception('递归次数超过10!')

    def get_data(self, url):
        resp = self.getData(url, self.a)
        return resp.text


    def get_interger(self, data):
        try:
            res = int(data)
        except:
            try:
                res = float(data)
            except:
                res = data
        return res
        
    def transfer_int(self,li):
        lis = []
        for i in li:
            lis.append(self.get_interger(i))
        return lis

    def my_xpath(self, data, ruler):
        return etree.HTML(data).xpath(ruler)

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
        
    def saveFile(self,seasonId):
        with open('C:\\sports\\football\\data\\season.json','a') as f:
            f.write(json.dumps('{},'.format(seasonId)))

    def current_season(self):
        seasonId_li = []
        seasonId_liu = []
        url = 'https://data.leisu.com/'
        resp = self.get_data(url)
        response = etree.HTML(resp)
        # div = response.xpath('//div[@class="left-list"]')
        for i in range(1,8):
            # area_name = response.xpath('//div[@class="left-list"]/div[{}]/div[@class="title"]/span/text()'.format(i))[0]
            # print(name)
            children_div = response.xpath('//div[@class="left-list"]/div[{}]/div'.format(i))
            if i == 1:
                ul = response.xpath('//div[@class="left-list"]/div[1]/div[3]/ul/li')
                for li in range(1, len(ul) + 1):
                    seasonId = response.xpath('//div[@class="left-list"]/div[1]/div[3]/ul/li[{}]/@data-season-id'.format(li))[0]
                    seasonId_liu.append(int(seasonId))
            else:
                for r in range(2, len(children_div) + 1):
                    # name_country = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/div[1]/span[@class="txt"]/text()'.format(i,r))
                    ul = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/ul/li'.format(i, r))
                    for li in range(1, len(ul) + 1):
                        seasonId = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/ul/li[{}]/@data-season-id'.format(i, r, li))[0]
                        seasonId_li.append(int(seasonId))
        tup = (seasonId_li, seasonId_liu)
        if not seasonId_li and not seasonId_liu:
            tup = self.current_season()
        return tup

    def request_web(self,seasonId,a):
        m = a
        url = 'https://data.leisu.com/zuqiu-{}'.format(seasonId)
        resp = self.get_data(url)
        leagueFullName = self.is_empty(self.my_xpath(resp, '//div[@class="clearfix-row f-s-24"]/text()'))
        seasonYear = self.is_empty(self.my_xpath(resp, '//div[@class="select-border"]/span[1]/span/text()'))
        teamNumber = self.is_empty(self.my_xpath(resp, '//ul[@class="head-list"]/li[2]/text()'))
        teamList = self.my_xpath(resp,'//div[@id="select-team-one"]/div[@class="down"]/ul/li/@data-id')
        if '-1' in teamList:
            teamList.remove('-1')
        teamList = self.transfer_int(teamList)
        if teamNumber:
            teamNumber = self.get_interger(teamNumber.split('：')[-1].strip())
            if not str(teamNumber).isdigit():
                teamNumber = ''
        tup = (leagueFullName,seasonYear,teamNumber,teamList)
        if not leagueFullName or not seasonYear:
            m += 1
            if m <= 10:
                tup = self.request_web(seasonId,m)
            else:
                logger.info('seasonId:{}-->该赛季没有匹配到\n'.format(seasonId))
        return tup

    def take_field(self,seasonId):
        try:
            tup = self.request_web(seasonId,self.a)
            leagueFullName,seasonYear,teamNumber,teamList = tup
            note_league = self.col_league.find_one({'name_zh':leagueFullName},{'leagueId':1,'short_name_zh':1,'areaId':1,'countryId':1})
            if note_league:
                leagueId = note_league.get('leagueId')
                short_name_zh = note_league.get('short_name_zh')
                areaId = note_league.get('areaId')
                countryId = note_league.get('countryId')
                note_area = self.col_area.find_one({'areaId':areaId},{'name_zh':1})
                note_country = self.col_country.find_one({'countryId':countryId},{'name_zh':1})
                if note_area:
                    if note_country:
                        country = note_country.get('name_zh')
                    else:
                        country = ''
                    area = note_area.get('name_zh')
                    item = {'leagueId':leagueId,'seasonId':seasonId,'area':area,'country':country,
                            'leagueName':short_name_zh,'seasonYear':seasonYear,'leagueFullName':leagueFullName,
                            'teamNumber':teamNumber,'teamList':teamList}
                    note_season = self.col_season.find_one({'seasonId':seasonId})
                    if not note_season:
                        # print(item)
                        self.col_season.insert_one(item)
                        print('已经插入一条season数据{}'.format(seasonId))
                    else:
                        self.col_season.update_one({'seasonId':seasonId},{'$set':{'teamList':teamList}})
                        print('已经更新一条season数据{}'.format(seasonId))
            # print('leagueFullName',leagueFullName,seasonYear)
        except Exception as e:
            # self.saveFile(seasonId)
            self.mdb.sadd('seasonTeam',seasonId)
            print('{}error:{}'.format(seasonId,e))
            
    def getSeason(self):
        for i in range(2,9257):#9257
            print('开始：',i)
            # note = self.col_season.find_one({'seasonId':i})
            # if not note:
            self.take_field(i)
    
    def li_season(self):
        # li,liu = self.current_season()
        li = ['9194','9157','9189','9187','9168','9170','9178','9174', '9162', '9183', '9135', '9148', '9185', '9151', '9191', '9196', '9180', '9192', '9181', '9193', '9173', '9190', '9176', '9136', '9179', '9197', '9184', '9177', '9171', '9143', '9142', '9122', '9169', '9172', '9186', '9129', '9182', '9195', '9198', '9199', '9188', '9175']
        for i in li:
            note = self.col_season.find_one({'seasonId':int(i)})
            if not note:
                print('开始：',i)
                self.take_field(int(i))
                
    def read_col(self):
        note = self.col_season.find({},{'_id':0}).sort([('seasonId',-1)]) #根据seasonId按从大到小顺序取出
        for i in note:
            self.col_renew_season.insert_one(i)
            print(i.get('seasonId'))
                
    def smiple(self):
        li = [9221]
        for i in li:
            note = self.col_season.find_one({'seasonId':i})
            print('开始：',i)
            if not note:
                self.take_field(i)
                
    def read_redis(self):
        while True:
            note = self.mdb.spop('seasonTeam')
            if note:
                self.take_field(int(note))
            else:
                break
if __name__ == '__main__':
    a = NewScore()
    a.getSeason()
    print('开始读取redis缓存')
    a.read_redis()
    # a.smiple()
    # a.read_col()
    
    
    # li,liu = a.current_season()
    # print(li)
    # print(len(li))