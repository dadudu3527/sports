import re
from lxml import etree
import time
import json
from datetime import datetime
# from multiprocessing.pool import ThreadPool
# from json.decoder import JSONDecodeError

import redis
import requests
# import pymongo
import random
import logging


from user_agent import agents
# from config import config


logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


class Match:
    def __init__(self):
        self.a = 0
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
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
            resp = requests.get(url=url, headers=headers, proxies=proxies)
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
            jieguo = data
        return jieguo

    def current_season(self):
        seasonId_li = []
        seasonId_liu = []
        url = 'https://data.leisu.com/'
        resp = self.get_data(url)
        response = etree.HTML(resp)
        # div = response.xpath('//div[@class="left-list"]')
        for i in range(1, 8):
            # area_name = response.xpath('//div[@class="left-list"]/div[{}]/div[@class="title"]/span/text()'.format(i))[0]
            # print(name)
            children_div = response.xpath('//div[@class="left-list"]/div[{}]/div'.format(i))
            if i == 1:
                ul = response.xpath('//div[@class="left-list"]/div[1]/div[3]/ul/li')
                for li in range(1, len(ul) + 1):
                    seasonId = \
                    response.xpath('//div[@class="left-list"]/div[1]/div[3]/ul/li[{}]/@data-season-id'.format(li))[0]
                    seasonId_liu.append(int(seasonId))
            else:
                for r in range(2, len(children_div) + 1):
                    # name_country = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/div[1]/span[@class="txt"]/text()'.format(i,r))
                    ul = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/ul/li'.format(i, r))
                    for li in range(1, len(ul) + 1):
                        seasonId = response.xpath(
                            '//div[@class="left-list"]/div[{}]/div[{}]/ul/li[{}]/@data-season-id'.format(i, r, li))[0]
                        seasonId_li.append(int(seasonId))
        tup = (seasonId_li, seasonId_liu)
        if not seasonId_li and not seasonId_liu:
            tup = self.current_season()
        return tup

    def quebao(self):
        url = 'https://live.leisu.com/wanchang'
        resp = self.get_data(url)
        season_lis = self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li/div[1]/div[1]/span[@class="lab-events"]/a/@href')
        if not season_lis:
            season_lis = self.quebao()
        return season_lis

    def renewMatch(self,season_set):
        season_lis = self.quebao()
        season_li = season_lis.copy()
        for i in season_set:
            if i in season_lis:
                season_lis.remove(i)
        while True:
            if 'javascript:void(0)' in season_lis:
                season_lis.remove('javascript:void(0)')
            else:
                break
        season_lis = [int(i.split('-')[-1].replace('/','')) for i in season_lis]
        season_url_s = set(season_lis)
        return season_li,tuple(season_url_s)
        
    def huanchong(self,season_url_s):
        self.mdb.sadd('seasonUrl',*season_url_s)
        

if __name__ == '__main__':
    a = Match()
    set1 = []
    while True:
        try:
            now = datetime.now()
            string = now.strftime('%Y-%m-%d %H:%M:%S')
            print('{}:开始更新season缓存......'.format(string))
            set1,season_url_s = a.renewMatch(set1)
            if season_url_s:
                a.huanchong(season_url_s)
            print('更新season缓存结束！')
            time.sleep(60*5)
        except Exception as e:
            print('发生错误：{}，将继续匹配！'.format(e))
    






   