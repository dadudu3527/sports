import requests
import pymongo
import random
from user_agent import agents
from multiprocessing.pool import ThreadPool
from lxml import etree


class EventRecommend:
    def __init__(self):
        client = pymongo.MongoClient()
        self.dbname = client['football']
        self.collection = self.dbname['matchIntelligence']
        # self.pool = ThreadPool(20)
        self.headers = {
            'User-Agent': random.choice(agents)
        }

    def get_data(self,url):
        resp = requests.get(url=url,headers=self.headers).text
        return resp

    def get_more(self,data):
        more = self.my_xpath(data,'//a[@class="more"]/@href')
        for i in more:
            url = 'https:'+ i
            res = self.get_data(url)
            matchId = self.is_empty(url.split('-')[-1])
            self.parse_data(res,matchId)

    def parse_data(self,data,matchId):
        td = self.my_xpath(data,'//div[@id="information"]/table//tr/td')
        matchTime = self.is_empty(self.my_xpath(data,'//div[@class="team-center"]/div[1]/text()'))
        # Team = self.my_xpath(data, '//div[@id="information"]/table//tr/td[{}]/div[1]/span/text()')
        item = {1: 'home', 3: 'away',}
        items = {'matchId': matchId,'matchTime': matchTime}
        for i in range(1,len(td)+1):
            if i == 1 or i == 3:
                advantage = self.my_xpath(data,'//div[@id="information"]/table//tr/td[{}]/div[@class="children good"]/ul/li/text()'.format(i))
                advantage_li = [{'type':'','title':'','description':ad.strip()} for ad in advantage]
                disadvantage = self.my_xpath(data,'//div[@id="information"]/table//tr/td[{}]/div[@class="children harmful"]/ul/li/text()'.format(i))
                disadvantage_li = [{'type': '', 'title': '', 'description': ad.strip()} for ad in disadvantage]
                items['{}Advantage'.format(item.get(i))] = advantage_li
                items['{}Disadvantage'.format(item.get(i))] = disadvantage_li
            else:
                neutral = self.my_xpath(data,'//div[@id="information"]/table//tr/td[2]/div/ul/li/text()')
                neutral_li = [{'level:': '', 'description': ad.strip()} for ad in neutral]
                items['neutral'] = neutral_li
        shuju = self.collection.find_one({'matchId':matchId})
        if shuju:
            self.collection.update_one({'matchId':matchId},{'$set':items})
        else:
            self.collection.insert_one(items)
        print(items)

    def my_xpath(self,data,rule):
        return etree.HTML(data).xpath(rule)

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
        else:
            jieguo = ''

        return jieguo

    def run(self):
        resp = self.get_data('https://guide.leisu.com/')
        self.get_more(resp)

if __name__ == '__main__':
    a = EventRecommend()
    a.run()
