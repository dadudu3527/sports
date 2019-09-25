import random
import time
from datetime import datetime
import logging

from lxml import etree
import requests
import pymongo
import redis

from config import config
from user_agent import agents

logger = logging.getLogger(__name__)
fh = logging.FileHandler('C:\\sports\\football\\data\\commend.log')
# sh = logging.StreamHandler()
logger.addHandler(fh)
# logger.addHandler(sh)
logger.setLevel(logging.DEBUG)



class EventRecommend:
    def __init__(self):
        self.a = 0
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True,password='fanfubao')
        client = pymongo.MongoClient(config)
        self.dbname = client['football']
        self.collection = self.dbname['matchanalysisfootball']
        # self.pool = ThreadPool(20)
        self.headers = {
            'User-Agent': random.choice(agents)
        }

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
        # headers = {'User-Agent': random.choice(agents)}
        # resp = requests.get(url=url, headers=headers)
        # if resp.status_code == 200:
            # pass
        # else:
        resp = self.getData(url, self.a)
        return resp.text

    def get_more(self,data):
        more = self.my_xpath(data,'//a[@class="more"]/@href')
        for i in more:
            url = 'https:'+ i
            self.take_commend(url)
            
    def take_commend(self,url):  
        matchId = self.is_empty(url.split('-')[-1])
        try:
            self.parse_data(url,matchId)  
        except Exception as e:
            self.mdb.sadd('commendURL',url)
            logger.info('{}：未请求成功！{}'.format(matchId,e))
            
    def get_td(self, url, a=0):
        res = self.get_data(url)
        td = self.my_xpath(res, '//div[@id="information"]/table//tr/td')
        if not td:
            a += 1
            if a <= 10:
                res = self.get_td(url,a=a)
            else:
                raise Exception('递归大于10！')
        return res
        
    def take_repeat(self): #情报去重
        shuju = self.collection.find({},{'_id':0})
        for items in shuju:
            matchId = items.get('gameid')
            for key in items.keys():
                if key == 'intelligence_away' or key == 'intelligence_home':
                    if items.get(key):
                        if items.get(key)[0].get('title'):
                            for p,i in enumerate(items.get(key)):
                                title = i.get('title')
                                if not title:
                                    items[key].remove(i)
                                    print('已删除重复数据。')
            self.collection.update_one({'gameid':matchId},{'$set':items})
        

    def parse_data(self, url, matchId):
        data = self.get_td(url)
        td = self.my_xpath(data, '//div[@id="information"]/table//tr/td')
        # matchTime = self.is_empty(self.my_xpath(data,'//div[@class="team-center"]/div[1]/text()'))
        # Team = self.my_xpath(data, '//div[@id="information"]/table//tr/td[{}]/div[1]/span/text()')
        item = {1: 'home', 3: 'away', }
        items = {'gameid': matchId,'intelligence_home': [], 'intelligence_bz': [], 'intelligence_away': []}
        for i in range(1,len(td)+1):
            if i == 1 or i == 3:
                advantage = self.my_xpath(data,'//div[@id="information"]/table//tr/td[{}]/div[@class="children good"]/ul/li/text()'.format(i))
                advantage_li = [{'class':'','title':'','type':'','type_c':'','text':ad.strip()} for ad in advantage]
                disadvantage = self.my_xpath(data,'//div[@id="information"]/table//tr/td[{}]/div[@class="children harmful"]/ul/li/text()'.format(i))
                disadvantage_li = [{'class':'bad','title': '','type': '','type_c':'','text': ad.strip()} for ad in disadvantage]
                advantage_li.extend(disadvantage_li)
                items['intelligence_{}'.format(item.get(i))] = advantage_li

            else:
                neutral = self.my_xpath(data,'//div[@id="information"]/table//tr/td[2]/div/ul/li/text()')
                neutral_li = [{'type': 'c', 'text': ad.strip()} for ad in neutral]
                items['intelligence_bz'] = neutral_li
        shuju = self.collection.find_one({'gameid':matchId})
        if shuju:
            if not shuju.get('intelligence_home') and not shuju.get('intelligence_away'):
                self.collection.update_one({'gameid':matchId},{'$set':items})
            # else:
                # for key in items.keys():
                    # if key == 'intelligence_away' or key == 'intelligence_home':
                        # if len(items[key])>len(shuju.get(key)):
                            # for p,i in enumerate(shuju.get(key)):
                                # title = i.get('title')
                                # type_1 = i.get('type')
                                # type_c = i.get('type_c')
                                # text = i.get('text')
                                # # if p < len(items[key]):
                                # items[key][p]['title'] = title
                                # items[key][p]['type'] = type_1
                                # items[key][p]['type_c'] = type_c
                                # items[key][p]['text'] = text
                                # # else:
                                    # # break 
                # self.collection.update_one({'gameid':matchId},{'$set':items})                    
        else:
            self.collection.insert_one(items)

    def my_xpath(self,data,rule):
        return etree.HTML(data).xpath(rule)
        
    def get_interger(self, data):
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
                if jieguo.isdigit():
                    jieguo = self.get_interger(jieguo)
            else:
                jieguo = ''
        else:
            jieguo = self.get_interger(data)
        return jieguo
        
    def swap_type(self):
        note = self.collection.find()
        for i in note:
            matchId = i.get('gameid')
            neu = []
            bz = i.get('intelligence_bz')
            if bz:
                bz_c = bz[0].get('type:')
                if bz_c == 'c':
                    for b in bz:
                        b['type'] = b.pop('type:')
                        neu.append(b)
                    self.collection.update({'gameid':matchId},{'$unset':{'intelligence_bz':''}},False, True)
                    self.collection.update_one({'gameid':matchId},{'$set':{'intelligence_bz':neu}})

    def run(self):
        resp = self.get_data('https://guide.leisu.com/')
        self.get_more(resp)
        

if __name__ == '__main__':
    a = EventRecommend()
    # a.swap_type()
    # a.take_repeat()
    while True:
        now = datetime.now()
        string = now.strftime('%Y-%m-%d %H:%M:%S')
        print('{}：情报数据下载中...'.format(string))
        try:
            a.run()
            # for i in range(20):
                # url = a.mdb.spop('commendURL')
                # if url:
                    # a.take_commend(url)
                # else:
                    # break
            print('情报数据下载完成')
            time.sleep(60*60*8)
        except Exception as e:
            print('请求超时，将继续请求！')
            
