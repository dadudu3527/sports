import requests
from lxml import etree

from user_agent import agents
from config import config

import time
import pymongo
import random
import logging
import re
import json

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class Statistics:
    def __init__(self):
        self.a = 0
        client = pymongo.MongoClient(config)
        dbname = client['football']
        self.col_read_team = dbname['team']
        self.col_read_player = dbname['player']
        self.collection_LT = dbname['leagueTopTeam']
        self.collection_PS = dbname['positionalStatistics']
        self.collection_SS = dbname['situationalStatistics']

    def my_xpath(self,data,ruler):
        return etree.HTML(data).xpath(ruler)

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

    def get_data(self,url):
        resp = self.getData(url,self.a)
        return resp.text

    def get_team_id(self,teamName):
        note = self.col_read_team.find_one({'name_zh': teamName}, {'teamId': 1})
        if note:
            teamId = note.get('teamId')
        else:
            note = self.col_read_team.find_one({'name_zh': {'$regex': teamName}}, {'teamId': 1})
            if note:
                teamId = note.get('teamId')
            else:
                teamId = ''
        return teamId

    def statistics(self,resp,leagueId,seasonName,name):  #数据类型分析
        li = ["goalType","shotType","passType"]
        item = {'leagueId': leagueId, 'season': seasonName,'leagueName':name}
        for i in li:
            tr_list = []
            tr = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr'.format(i))
            for r in range(1,len(tr)+1):
                td_list = []
                td = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td'.format(i,r))
                for m in range(2,len(td)+1):
                    if m == 2:
                        teamName = self.is_empty(self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[2]/a/text()'.format(i,r)))
                        td_list.append(teamName)
                        teamId = self.get_team_id(teamName)
                        td_list.append(teamId)
                    else:
                        zdx = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[{}]/text()'.format(i,r,m))
                        zdx = self.get_interger(''.join(zdx).replace('\r', '').replace('\n', '').replace('\t','').replace(' ', ''))
                        td_list.append(zdx)
                tr_list.append(td_list)
            item[i] = tr_list
        # print(item)
        return item
    def positionalStatistics(self,resp,leagueId,seasonName,name): #球队进攻分布
        li = ["attackSide","shotDirection","shotZone"]
        item = {'leagueId': leagueId, 'season': seasonName,'leagueName':name}
        for i in li:
            tr_list = []
            tr = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr'.format(i))
            for r in range(1,len(tr)+1):
                td_list = []
                td = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td'.format(i,r))
                for m in range(2,len(td)+1):
                    if m == 2:
                        teamName = self.is_empty(self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[2]/a/text()'.format(i,r)))
                        td_list.append(teamName)
                        teamId = self.get_team_id(teamName)
                        td_list.append(teamId)
                    else:
                        zdx = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[{}]/span/span/span/text()'.format(i,r,m))
                        zdx = self.get_interger(''.join(zdx).replace('\r', '').replace('\n', '').replace('\t','').replace(' ', ''))
                        td_list.append(zdx)
                tr_list.append(td_list)
            item[i] = tr_list
        # print(item)
        return item

    def leagueTopTeam(self,leagueid,leagueId,seasonName,name):
        url = 'http://www.tzuqiu.cc/competitions/{}/show.do'.format(leagueid)
        resp  = self.get_data(url)
        dic = {
            'seasonTeamStat':'dataOfSeason',
            'roundTeamStat':'dataOfRound',
            '控球率':'possesion',
            '传球成功率':'passAccuracy',
            '绝佳机会':'greatOpportunity',
            '场均射门':'shootPerGame',
            '红黄牌':'aggression',
            '评分':'rating',
            '射门次数':'shootNumber',
        }
        li = ["seasonTeamStat", "roundTeamStat"]
        item = {'leagueId': leagueId, 'season': seasonName,'leagueName':name}
        for i in li:
            tr_item = {}
            div = self.my_xpath(resp, '//div[@id="{}"]/div'.format(i))
            for r in range(1, len(div) + 1):
                div_1 = self.my_xpath(resp,'//div[@id="{}"]/div[{}]/div'.format(i,r))
                for p in range(1,len(div_1)+1):
                    tr_list = []
                    th_name = self.is_empty(self.my_xpath(resp, '//div[@id="{}"]/div[{}]/div[{}]/table/thead/tr/th/text()'.format(i, r, p)))
                    tr = self.my_xpath(resp, '//div[@id="{}"]/div[{}]/div[{}]/table/tbody/tr'.format(i, r, p))
                    for m in range(1, len(tr) + 1):
                        td_list = []
                        teamName = self.is_empty(self.my_xpath(resp,'//div[@id="{}"]/div[{}]/div[{}]/table/tbody/tr[{}]/td[1]/a/text()'.format(i, r, p, m)))
                        td_list.append(teamName)
                        teamId = self.get_team_id(teamName)
                        td_list.append(teamId)
                        teamValue = self.my_xpath(resp,'//div[@id="{}"]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span/span/span/text()'.format(i, r, p, m))
                        if teamValue:
                            teamValue = self.get_interger(''.join(teamValue).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', ''))
                            td_list.append(teamValue)
                        else:
                            teamValue = self.my_xpath(resp,'//div[@id="{}"]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span/text()'.format(i, r, p, m))
                            teamValue = self.transfer_int(teamValue)
                            td_list.extend(teamValue)
                        tr_list.append(td_list)
                    tr_item[dic.get(th_name)] = tr_list
            item[dic.get(i)] = tr_item
        return item

    def get_interger(self,data):
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

    def is_empty(self, data):  # 处理数据为空的字段 str转int float
        if data != [] and isinstance(data,list):
            jieguo = data[0].strip()
            if jieguo.isdigit():
                jieguo = int(jieguo)
            else:
                if not jieguo.isalpha():
                    try:
                        jieguo = float(jieguo)
                    except:
                        pass
        elif isinstance(data,str):
            jieguo = data
            if jieguo.isdigit():
                jieguo = int(jieguo)
            else:
                if not jieguo.isalpha():
                    try:
                        jieguo = float(jieguo)
                    except:
                        pass
        elif isinstance(data, int):
            jieguo = data
        elif isinstance(data, float):
            jieguo = data
        else:
            jieguo = ''
        return jieguo

    def get_url(self):
        resp = self.get_data('http://www.tzuqiu.cc/')
        url_list = []
        leagueName_list = []
        li = ['competition-league-list','competition-cup-list']
        for i in li:
            url = self.my_xpath(resp,'//div[@id="{}"]/ul/li/a/@href'.format(i))
            url_list.extend(url)
            leagueName = self.my_xpath(resp,'//div[@id="{}"]/ul/li/a/text()'.format(i))
            leagueName_list.extend(leagueName)
        url_list = ['http://www.tzuqiu.cc'+i for i in url_list]
        return zip(leagueName_list,url_list)

    def parse_leagueId(self,data):
        item = self.league()
        for name,url in data:
            if name == '欧洲杯预选赛':
                continue
            leagueid = re.findall(r'http://www\.tzuqiu\.cc/competitions/(.*?)/show\.do',url)[0] #str
            leagueid = int(leagueid)
            leagueId = item.get(name)
            self.run(leagueid,leagueId,name)

    def league(self):
        item = {
        '英格兰足球超级联赛':82,
        '西班牙足球甲级联赛':120,
        '意大利足球甲级联赛':108,
        '德国足球甲级联赛':129,
        '法国足球甲级联赛':142,
        '巴西足球甲级联赛':435,
        '荷兰足球甲级联赛':168,
        '葡萄牙足球超级联赛':151,
        '英格兰足球冠军联赛':83,
        '俄罗斯足球超级联赛':238,
        '土耳其足球超级联赛':315,
        '阿根廷足球甲级联赛':429,
        '中国足球超级联赛':542,
        '欧洲冠军联赛':46,
        '欧洲联赛':47,
        '2018世界杯':1,
        '美洲杯':395,
        '非洲杯':700,
        '亚洲冠军联赛':491,
        '英格兰足总杯':98,
        '英格兰联赛杯':99,
        '西班牙国王杯':125,
        '意大利杯':114,
        '德国杯':136,
        '法国联赛杯':149,
        '法国杯':148,
        '欧洲杯':45,
        '国际友谊赛':34,
        '世界杯欧洲区预选赛':2,
        '欧洲国家联赛A级': 2906,
        '欧洲国家联赛B级':2906,
        '欧洲国家联赛C级':2906,
        '欧洲国家联赛D级':2906,
        '亚洲杯':490
        }
        return item

    def saveFile(self, teamidT):
        with open('C:\\sports\\football\\data\\league_Tz.json', 'a') as f:
            f.write(json.dumps(teamidT, ensure_ascii=False) + '/,')

    def save_database(self,collect, id, leagueData):
        note = collect.find_one({'leagueId':id,'season':leagueData.get('season')})
        if not note:
            collect.insert_one(leagueData)
        else:
            collect.update_one({'leagueId':id,'season':leagueData.get('season')},{'$set': leagueData})

    def run(self,id,leagueId,name,season='18/19'):
        logger.info('{}:开始下载...'.format(leagueId))
        try:
            res = self.get_data('http://www.tzuqiu.cc/competitions/{}/teamStats.do'.format(id))
            seasonName = self.my_xpath(res, '/html/head/title/text()')[0]
            seasonName = re.findall(r'.*?球队数据\|(\d+/\d+).*?球队数据\|.*?球队实力排名-T足球', seasonName)
            if not seasonName:
                seasonName = season
            else:
                seasonName = seasonName[0]
            SS = self.statistics(res,leagueId,seasonName,name)
            self.save_database(self.collection_SS,leagueId,SS)

            PS = self.positionalStatistics(res,leagueId,seasonName,name)
            self.save_database(self.collection_PS, leagueId, PS)

            LT = self.leagueTopTeam(id,leagueId,seasonName,name)
            self.save_database(self.collection_LT, leagueId, LT)
            logger.info('{}♥数据下载成功'.format(leagueId))
        except Exception as e:
            self.saveFile({'id':id,'leagueId':leagueId,'name':name})
            logger.info('{}:下载失败！-->{}'.format(leagueId,e))

if __name__ == '__main__':
    a = Statistics()
    # a.run()
    while True:
        a.parse_leagueId(a.get_url())
        with open('C:\\sports\\football\\data\\league_Tz.json','r') as f:
            id_str = f.read()
            if id_str:
                id_list = set(id_str.split('/,'))
                for i in id_list:
                    if i != '':
                        d = json.loads(i)
                        a.run(d.get('id'),d.get('leagueId'),d.get('name'))
        time.sleep(60*60*24*3)
        with open('C:\\sports\\football\\data\\playerid_Tz.json','w') as f:
            f.write('')
            
            
            