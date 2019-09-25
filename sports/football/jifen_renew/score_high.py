from multiprocessing.pool import ThreadPool
import re
import time
import json
import logging
from datetime import datetime
import random

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import requests
import redis
import pymongo
from lxml import etree

from user_agent import agents
from config import config

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


# pool = ThreadPool(2)

class TiyuData:
    a = 0
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(executable_path='chromedriver1.exe', chrome_options=chrome_options)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True, password='fanfubao')
        client = pymongo.MongoClient(config)
        self.dbname = client['football']
        self.col_team = self.dbname['team']
        self.collection_error = self.dbname['score_error']
        self.col_read = self.dbname['season']
        self.collection_jf = self.dbname['scoreBoard']  # 积分榜
        self.collection_fjf = self.dbname['groupScoreBoard']  # 分组积分榜
        self.collection_ss = self.dbname['playerGoalTable']  # 射手榜
        self.collection_zg = self.dbname['playerAssistTable']  # 助攻榜
        self.collection_fs = self.dbname['playerDefensiveTable']  # 防守榜
        self.collection_qd = self.dbname['TeamPerformance']  # 球队数据
        self.collection_rt = self.dbname['totalAsiaHandicap']  # 让球总盘路
        self.collection_rh = self.dbname['totalHomeAsiaHandicap']  # 让球主场盘路
        self.collection_ra = self.dbname['totalAwayAsiaHandicap']  # 让球客场盘路
        self.collection_rht = self.dbname['halfAsiaHandicap']  # 让球半场总盘路
        self.collection_rhh = self.dbname['halfHomeAsiaHandicap']  # 让球半场主场盘路
        self.collection_rha = self.dbname['halfAwayAsiaHandicap']  # 让球半场客场盘路
        self.collection_jt = self.dbname['totalGoalLine']  # 进球数总盘路
        self.collection_jh = self.dbname['totalHomeGoalLine']  # 进球数主场盘路
        self.collection_ja = self.dbname['totalAwayGoalLine']  # 进球数客场盘路
        self.collection_jht = self.dbname['halfGoalLine']  # 进球数半场总盘路
        self.collection_jhh = self.dbname['halfHomeGoalLine']  # 进球数半场主场盘路
        self.collection_jha = self.dbname['halfAwayGoalLine']  # 进球数半场客场盘路
        self.collection_bt = self.dbname['halfFullResult']  # 半全场总计
        self.collection_bh = self.dbname['halfFullHomeResult']  # 半全场主场
        self.collection_ba = self.dbname['halfFullAwayResult']  # 半全场客场

    def get_data(self, url):
        self.driver.get(url)  # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="clearfix-row m-t-10"]/div[2]')))
        # time.sleep(3)

    def my_xpath(self, data, ruler):
        return etree.HTML(data).xpath(ruler)

    def getLeagueId(self, seasonId):  #取消注释
        note = self.col_read.find_one({'seasonId': seasonId})
        if note:
            leagueId = note.get('leagueId')
        else:
            raise Exception('没有获取到leagueId')
        return leagueId

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

    def requests_data(self, url):
        resp = self.getData(url, self.a)
        return resp.text

    def requests_jifen(self,seasonId,leagueId):
        url = 'https://data.leisu.com/zuqiu-{}'.format(seasonId)
        page = self.requests_data(url)
        tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tr[@class="data pd-8"]')
        d_list = []
        if tr:
            content_paiming = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tr[@class="data pd-8"]/td[1]/span/text()')
            content_qiuyuan = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tr[@class="data pd-8"]/td[2]/a/@href')
            content_ = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tr[@class="data pd-8"]/td/text()')
            content_.append(88888)
            # print(content_paiming,len(content_paiming))
            # print(content_qiuyuan,len(content_qiuyuan))
            # print(content_,len(content_))
            li = []
            lis = []
            for p, i in enumerate(content_):
                if p != 0 and p % 13 == 0:
                    lis.append(li)
                    li = []
                li.append(i)
            # print(lis,len(lis))
            length = len(content_) - 1
            if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 13:
                for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                    teamId = url.split('-')[-1].strip()
                    item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId),
                            'total': self.get_interger(content[2].strip()),
                            'win': self.get_interger(content[3].strip()), 
                            'draw': self.get_interger(content[4].strip()),
                            'lose': self.get_interger(content[5].strip()),
                            'goal': self.get_interger(content[6].strip()),
                            'goalAgainst': self.get_interger(content[7].strip()),
                            'points': self.get_interger(content[-1].strip()), }
                    d_list.append(item)
            else:
                raise Exception('积分榜数据长度匹配异常！')
        items = {
            'leagueId':leagueId,
            'seasonId':seasonId,
            'round':'总积分',
            'tableDetail':d_list,
            'tableDetailHome':[],
            'tableDetailAway':[],
            'tableDetailHalf':[],
            'tableDetailHomeHalf':[],
            'tableDetailAwayHalf':[],
        }
        data = self.collection_jf.find_one({'seasonId': seasonId, 'round': items.get('round')})   #取消注释
        if not data:
            self.collection_jf.insert_one(items)
        else:
            self.collection_jf.update_one({'seasonId': seasonId, 'round': items.get('round')},{'$set': items})
        # print(items)
        logger.info('seasonId{}:积分榜下载成功'.format(seasonId))
        return items

    def parse_data_fenzu(self, seasonId, leagueId):  # 分组积分
        page = self.driver.page_source
        a = False
        stageId_list = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div/@data-stage')
        stageId_li = list(set(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div/@data-stage')))
        stageId_li.sort(key=stageId_list.index)  # set集合去重无序，此方法可以使stageId_li按照set去重之前的顺序排列
        for p, stageId in enumerate(stageId_li):
            stageId = int(stageId)
            stageName = self.is_empty(self.my_xpath(page, '//a[@data-id="{}"]/text()'.format(stageId)))
            items = {'leagueId': leagueId, 'seasonId': seasonId, 'stageId': stageId , 'stageName': stageName}
            groupTable = []
            div_list_now = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(stageId))
            if p == 0:
                for i in range(1, len(div_list_now) + 1):  # (len(div_list))
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(
                        self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        content_paiming = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[1]/span/text()'.format(i))
                        content_qiudui = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[2]/a/@href'.format(i))
                        content_ = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td/text()'.format(i))
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiudui,len(content_qiudui))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 13 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiudui) == len(lis) == length / 13:
                            for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                teamId = url.split('-')[-1].strip()
                                item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId),
                                        'played': self.get_interger(content[-11].strip()),'win':self.get_interger(content[-10].strip()),
                                        'draw':self.get_interger(content[-9].strip()),'lose': self.get_interger(content[-8].strip()),
                                        'goal': self.get_interger(content[-7].strip()), 'goalAgainst': self.get_interger(content[-6].strip()),
                                        'points': self.get_interger(content[-1].strip())}
                                rank_List.append(item)
                                if title == '':
                                    a = True
                                    break
                        else:
                            raise Exception('射手榜数据长度匹配异常！')
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            elif p == 1:
                div_list_before = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(
                    stageId_li[p - 1]))
                for i in range(len(div_list_before) + 1, len(div_list_now) + len(div_list_before) + 1):
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(
                        self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page,
                                       '//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(
                        self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        content_paiming = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[1]/span/text()'.format(i))
                        content_qiudui = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[2]/a/@href'.format(i))
                        content_ = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td/text()'.format(i))
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiudui,len(content_qiudui))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 13 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiudui) == len(lis) == length / 13:
                            for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                teamId = url.split('-')[-1].strip()
                                item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId),
                                        'played': self.get_interger(content[-11].strip()),
                                        'win': self.get_interger(content[-10].strip()), 'draw': self.get_interger(content[-9].strip()),
                                        'lose': self.get_interger(content[-8].strip()), 'goal': self.get_interger(content[-7].strip()),
                                        'goalAgainst': self.get_interger(content[-6].strip()), 'points': self.get_interger(content[-1].strip())}
                                rank_List.append(item)
                                if title == '':
                                    a = True
                                    break
                        else:
                            raise Exception('射手榜数据长度匹配异常！')
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            elif p == 2:
                div_list_before = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(
                    stageId_li[p - 1]))
                div_list_before2 = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(
                    stageId_li[p - 2]))
                for i in range(len(div_list_before) + len(div_list_before2) + 1,
                               len(div_list_now) + len(div_list_before) + len(div_list_before2) + 1):
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(
                        self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page,
                                       '//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(
                        self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        content_paiming = self.my_xpath(page,
                                                        '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[1]/span/text()'.format(
                                                            i))
                        content_qiudui = self.my_xpath(page,
                                                       '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td[2]/a/@href'.format(
                                                           i))
                        content_ = self.my_xpath(page,
                                                 '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr/td/text()'.format(
                                                     i))
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiudui,len(content_qiudui))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 13 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiudui) == len(lis) == length / 13:
                            for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                teamId = url.split('-')[-1].strip()
                                item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId),
                                        'played': self.get_interger(content[-11].strip()),
                                        'win': self.get_interger(content[-10].strip()), 'draw': self.get_interger(content[-9].strip()),
                                        'lose': self.get_interger(content[-8].strip()), 'goal': self.get_interger(content[-7].strip()),
                                        'goalAgainst': self.get_interger(content[-6].strip()), 'points': self.get_interger(content[-1].strip())}
                                rank_List.append(item)
                                if title == '':
                                    a = True
                                    break
                        else:
                            raise Exception('射手榜数据长度匹配异常！')
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            else:
                print('分组赛没有匹配完整！')
            items['groupTable'] = groupTable
            # print(items)
            data = self.collection_fjf.find_one({'seasonId': seasonId,'stageId':stageId})
            if not data:
                self.collection_fjf.insert_one(items)
            else:
                self.collection_fjf.update_one({'seasonId': seasonId,'stageId':stageId}, {'$set': items})
            logger.info('seasonId{}:分组积分下载成功'.format(seasonId))

    def parse_data_jifen(self, seasonId, leagueId):  # 积分榜
        wait = WebDriverWait(self.driver, 10)
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div[1]/div[1]/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        tihuan = {'总积分': 'tableDetail', '主场积分': 'tableDetailHome', '客场积分': 'tableDetailAway',
                  '半场总积分': 'tableDetailHalf', '半场主场积分': 'tableDetailHomeHalf', '半场客场积分': 'tableDetailAwayHalf'}
        if not div_list:
            self.parse_data_fenzu(seasonId, leagueId)
        else:
            page = self.driver.page_source
            gg = self.my_xpath(page, '//*[@id="stages-nav"]/a[1]')
            if gg:
                jifen = self.driver.find_element_by_xpath('//*[@id="stages-nav"]/a[1]')
                jifen.click()

            for y, lun in enumerate(all_score):  # y索引all_score列表
                items = {}
                try:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="match-nav-list m-t-15"]/div[1]/div[1]')))
                    res.click()
                except:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/div')))
                    res.click()
                try:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH,'//div[@class="table-list"]/div[1]/div[1]/div[1]/div[1]/ul/li[{}]/a'.format(y + 1))))  # 实现轮次的点击
                    lunci.click()
                except:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH,'//div[@class="table-list"]/div[1]/div[1]/div[1]/div[1]/ul/li[{}]'.format(y + 1))))  # 实现轮次的点击
                    lunci.click()

                chc = lun.get_attribute('text')
                # teamList = []
                items['leagueId'] = leagueId
                items['seasonId'] = seasonId
                if chc != '总积分':
                    items['round'] = int(chc.replace('第', '').replace('轮', ''))
                else:
                    items['round'] = chc
                for div in range(1, len(div_list) + 1):
                    wait.until(EC.presence_of_element_located((By.ID, "scoreboard")))
                    a = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[{}]/div/a'.format(div))
                    for p, i in enumerate(a):
                        i.click()
                        wait.until(EC.presence_of_all_elements_located(
                            (By.XPATH, '//*[@id="scoreboard"]/div[1]/table/tbody/tr')))
                        jifen_type = i.get_attribute('text')
                        page = self.driver.page_source
                        if p == 0 and y == 0:
                            tr = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]'.format(div))
                            d_list = []
                            if tr:
                                content_paiming = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td[1]/span/text()'.format(div))
                                content_qiudui = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td[2]/a/@href'.format(div))
                                content_ = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td/text()'.format(div))
                                content_.append(88888)
                                # print(content_paiming,len(content_paiming))
                                # print(content_qiuyuan,len(content_qiuyuan))
                                # print(content_,len(content_))
                                li = []
                                lis = []
                                for p, i in enumerate(content_):
                                    if p != 0 and p % 13 == 0:
                                        lis.append(li)
                                        li = []
                                    li.append(i)
                                # print(lis,len(lis))
                                length = len(content_) - 1
                                if len(content_paiming) == len(content_qiudui) == len(lis) == length / 13:
                                    for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                        teamId = url.split('-')[-1].strip()
                                        item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId),
                                            'total': self.get_interger(content[2].strip()),
                                            'win': self.get_interger(content[3].strip()),
                                            'draw': self.get_interger(content[4].strip()),
                                            'lose': self.get_interger(content[5].strip()),
                                            'goal': self.get_interger(content[6].strip()),
                                            'goalAgainst': self.get_interger(content[7].strip()),
                                            'points': self.get_interger(content[-1].strip()), }
                                        d_list.append(item)
                                else:
                                    raise Exception('积分榜数据长度匹配异常！')
                        else:
                            tr = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]'.format(div))
                            d_list = []
                            if tr:
                                content_paiming = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/span/text()'.format(div))
                                content_qiudui = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/@href'.format(div))
                                content_ = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td/text()'.format(div))
                                content_.append(88888)
                                # print(content_paiming,len(content_paiming))
                                # print(content_qiuyuan,len(content_qiuyuan))
                                # print(content_,len(content_))
                                li = []
                                lis = []
                                for p, i in enumerate(content_):
                                    if p != 0 and p % 11 == 0:
                                        lis.append(li)
                                        li = []
                                    li.append(i)
                                # print(lis,len(lis))
                                length = len(content_) - 1
                                if len(content_paiming) == len(content_qiudui) == len(lis) == length / 11:
                                    for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                        teamId = url.split('-')[-1].strip()
                                        item = {
                                            'rank': int(rank.strip()),
                                            'teamId': self.get_interger(teamId),
                                            'total': self.get_interger(content[0].strip()),
                                            'win': self.get_interger(content[1].strip()),
                                            'draw': self.get_interger(content[2].strip()),
                                            'lose': self.get_interger(content[3].strip()),
                                            'goal': self.get_interger(content[4].strip()),
                                            'goalAgainst': self.get_interger(content[5].strip()),
                                            'points': self.get_interger(content[-1].strip()),
                                        }
                                        d_list.append(item)
                                else:
                                    raise Exception('积分榜数据长度匹配异常。')
                        items[tihuan.get(jifen_type)] = d_list
                    data = self.collection_jf.find_one({'seasonId': seasonId,'round':items.get('round')})
                    if not data:
                        self.collection_jf.insert_one(items)
                    else:
                        self.collection_jf.update_one({'seasonId': seasonId,'round':items.get('round')}, {'$set': items})
                    # print(items)
                    logger.info('seasonId{}:积分榜下载成功'.format(seasonId))

    def jifenbang(self, seasonId, leagueId):  # 积分榜
        wait = WebDriverWait(self.driver, 10)
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        tihuan = {'总积分': 'tableDetail', '主场积分': 'tableDetailHome', '客场积分': 'tableDetailAway',
                  '半场总积分': 'tableDetailHalf', '半场主场积分': 'tableDetailHomeHalf', '半场客场积分': 'tableDetailAwayHalf'}
        if not div_list:
            self.parse_data_fenzu(seasonId, leagueId)
        else:
            page = self.driver.page_source
            gg = self.my_xpath(page, '//*[@id="stages-nav"]/a[1]')
            if gg:
                jifen = self.driver.find_element_by_xpath('//*[@id="stages-nav"]/a[1]')
                jifen.click()

            for y in range(2):  # 总轮次和当前最后一轮
                dic = {0:1,1:'last()'}
                items = {}
                try:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="match-nav-list m-t-15"]/div[1]/div[1]')))
                    res.click()
                except:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/div')))
                    res.click()
                try:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH,'//div[@class="table-list"]/div[1]/div[1]/div[1]/div[1]/ul/li[{}]/a'.format(dic.get(y)))))  # 实现轮次的点击
                    lunci.click()
                except:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH,'//div[@class="table-list"]/div[1]/div[1]/div[1]/div[1]/ul/li[{}]'.format(dic.get(y)))))  # 实现轮次的点击
                    lunci.click()

                chc = lunci.get_attribute('text')
                # teamList = []
                items['leagueId'] = leagueId
                items['seasonId'] = seasonId
                if chc != '总积分':
                    items['round'] = int(chc.replace('第', '').replace('轮', ''))
                else:
                    items['round'] = chc
                for div in range(1, len(div_list) + 1):
                    wait.until(EC.presence_of_element_located((By.ID, "scoreboard")))
                    a = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[{}]/div/a'.format(div))
                    for p, i in enumerate(a):
                        i.click()
                        wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="scoreboard"]/div[1]/table/tbody/tr')))
                        jifen_type = i.get_attribute('text')
                        page = self.driver.page_source
                        if p == 0 and y == 0:
                            tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]'.format(div))
                            d_list = []
                            if tr:
                                content_paiming = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td[1]/span/text()'.format(div))
                                content_qiuyuan = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td[2]/a/@href'.format(div))
                                content_ = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8"]/td/text()'.format(div))
                                content_.append(88888)
                                # print(content_paiming,len(content_paiming))
                                # print(content_qiuyuan,len(content_qiuyuan))
                                # print(content_,len(content_))
                                li = []
                                lis = []
                                for p, i in enumerate(content_):
                                    if p != 0 and p % 13 == 0:
                                        lis.append(li)
                                        li = []
                                    li.append(i)
                                # print(lis,len(lis))
                                length = len(content_) - 1
                                if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 13:
                                    for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                                        teamId = url.split('-')[-1].strip()
                                        item = {
                                            'rank': int(rank.strip()),
                                            'teamId': self.get_interger(teamId),
                                            'total': self.get_interger(content[2].strip()),
                                            'win': self.get_interger(content[3].strip()),
                                            'draw': self.get_interger(content[4].strip()),
                                            'lose': self.get_interger(content[5].strip()),
                                            'goal': self.get_interger(content[6].strip()),
                                            'goalAgainst':self.get_interger(content[7].strip()),
                                            'points':self.get_interger(content[-1].strip()),
                                        }
                                        d_list.append(item)
                                else:
                                    raise Exception('积分榜数据长度匹配异常！')
                        else:
                            tr = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]'.format(div))
                            d_list = []
                            if tr:
                                content_paiming = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/span/text()'.format(div))
                                content_qiuyuan = self.my_xpath(page, '//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/@href'.format(div))
                                content_ = self.my_xpath(page,'//*[@id="scoreboard"]/div[{}]/table/tbody/tr[@class="data pd-8 temporary"]/td/text()'.format(div))
                                content_.append(88888)
                                # print(content_paiming,len(content_paiming))
                                # print(content_qiuyuan,len(content_qiuyuan))
                                # print(content_,len(content_))
                                li = []
                                lis = []
                                for p, i in enumerate(content_):
                                    if p != 0 and p % 11 == 0:
                                        lis.append(li)
                                        li = []
                                    li.append(i)
                                # print(lis,len(lis))
                                length = len(content_) - 1
                                if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 11:
                                    for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                                        teamId = url.split('-')[-1].strip()
                                        item = {
                                            'rank': int(rank.strip()),
                                            'teamId': self.get_interger(teamId),
                                            'total': self.get_interger(content[0].strip()),
                                            'win': self.get_interger(content[1].strip()),
                                            'draw': self.get_interger(content[2].strip()),
                                            'lose': self.get_interger(content[3].strip()),
                                            'goal': self.get_interger(content[4].strip()),
                                            'goalAgainst': self.get_interger(content[5].strip()),
                                            'points': self.get_interger(content[-1].strip()),
                                        }
                                        d_list.append(item)
                                else:
                                    raise Exception('积分榜数据长度匹配异常。')
                        items[tihuan.get(jifen_type)] = d_list
                    
                    data = self.collection_jf.find_one({'seasonId': seasonId,'round':items.get('round')})
                    if not data:
                        self.collection_jf.insert_one(items)
                    else:
                        self.collection_jf.update_one({'seasonId': seasonId,'round':items.get('round')},{'$set': items})
                    # print(items)
                    logger.info('seasonId{}:积分榜下载成功'.format(seasonId))

    def parse_data_qiudui(self, seasonId, leagueId):  # 球队球员数据
        # col_item = {'射手榜':'playerGoalTable','助攻榜':'playerAssistTable','球员防守':'playerDefensiveTable','球队数据':'TeamPerformance'}
        # leagueId = self.getLeagueId(seasonId)
        wait = WebDriverWait(self.driver, 5)
        t_item = {'射手榜': 'shooterTable', '助攻榜': 'assistTable', '球员防守': 'DefendTable', '球队数据': 'performData'}
        try:
            qiudui_y= self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[2]')
            qiudui_y.click()
        except:
            try:
                qiudui_y = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//a[@id="shooter-list"]')))
                qiudui_y.click()
            except:
                try:
                    qiudui_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list m-t-15"]/a[2]')
                    qiudui_y.click()
                except:
                    try:
                        qiudui_y = self.driver.find_element_by_xpath(
                            '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[1]/a[2]')
                        qiudui_y.click()
                    except:
                        qiudui_y = self.driver.find_element_by_xpath(
                        '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[2]')
                        qiudui_y.click()
        # a_list = self.driver.find_elements_by_xpath('//*[@id="shooter-list"]/div[1]/a')
        a_list = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="shooter-list"]/div[1]/a')))
        if a_list:
            for p, i in enumerate(a_list):
                items = {'id': p + 1, 'leagueId': leagueId, 'seasonId': seasonId}
                rank_list = []
                i.click()
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="shooter-list"]/div[{}]/table/tbody/tr'.format(p+2))))
                name = i.get_attribute('text')
                print(name)
                page = self.driver.page_source
                if p == 0:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[2]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        content_paiming = self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[@class="pd-8"]/td[1]/span/text()')
                        content_qiuyuan = self.my_xpath(page, '//*[@id="shooter-list"]/div[2]/table/tbody/tr[@class="pd-8"]/td[2]/a/@href')
                        content_ = self.my_xpath(page, '//*[@id="shooter-list"]/div[2]/table/tbody/tr[@class="pd-8"]/td/text()')
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiuyuan,len(content_qiuyuan))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 8 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 8:
                            for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                                playerId = url.split('-')[-1].strip()
                                jinqiu = content[-2].strip()
                                if jinqiu.isdigit():
                                    goal = int(jinqiu)
                                    penaltyGoal = ''
                                else:
                                    goal = int(jinqiu.split('(')[0].strip())
                                    penaltyGoal = int(jinqiu.split('(')[-1].split(')')[0])

                                item = {'rank': int(rank.strip()), 'playerId': self.get_interger(playerId), 'attendenceNo': self.get_interger(content[-4].strip()),
                                        'attandenceMinute': self.get_interger(content[-3].strip()), 'goal': goal, 'penaltyGoal': penaltyGoal,
                                        'goalConsum': self.get_interger(content[-1].strip())}
                                rank_list.append(item)
                        else:
                            raise Exception('射手榜数据长度匹配异常！')
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_ss, seasonId, items) #取消注释
                    # print(items)
                    logger.info('seasonId{}:射手榜下载成功'.format(seasonId))

                elif p == 1:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        content_paiming = self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]/td[1]/span/text()')
                        content_qiuyuan = self.my_xpath(page, '//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]/td[2]/a/@href')
                        content_ = self.my_xpath(page, '//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]/td/text()')
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiuyuan,len(content_qiuyuan))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 7 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 7:
                            for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                                playerId = url.split('-')[-1].strip()
                                item = {'rank': int(rank.strip()), 'playerId': self.get_interger(playerId), 'passNo': self.get_interger(content[-3].strip()),
                                        'keyPassNo': self.get_interger(content[-2].strip()), 'assistNo': self.get_interger(content[-1].strip())}
                                rank_list.append(item)
                        else:
                            raise Exception('助攻榜数据长度匹配异常！')
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_zg, seasonId, items) #取消注释
                    # print(items)
                    logger.info('seasonId{}:助攻榜下载成功'.format(seasonId))
                elif p == 2:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        content_paiming = self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]/td[1]/span/text()')
                        content_qiuyuan = self.my_xpath(page, '//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]/td[2]/a/@href')
                        content_ = self.my_xpath(page, '//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]/td/text()')
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiuyuan,len(content_qiuyuan))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 7 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiuyuan) == len(lis) == length / 7:
                            for rank, url, content in zip(content_paiming, content_qiuyuan, lis):
                                playerId = url.split('-')[-1].strip()
                                item = {'rank': int(rank.strip()), 'playerId': self.get_interger(playerId), 'attendenceNo': self.get_interger(content[-3].strip()),
                                        'stealNo': self.get_interger(content[-2].strip()), 'clearanceNo':self.get_interger(content[-1].strip())}
                                rank_list.append(item)
                        else:
                            raise Exception('球员防守榜数据长度匹配异常！')
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_fs, seasonId, items) #取消注释
                    # print(items)
                    logger.info('seasonId{}:球员防守榜下载成功'.format(seasonId))

                elif p == 3:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        content_paiming = self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]/td[1]/span/text()')
                        content_qiudui = self.my_xpath(page, '//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]/td[2]/a/@href')
                        content_ = self.my_xpath(page, '//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]/td/text()')
                        content_.append(88888)
                        # print(content_paiming,len(content_paiming))
                        # print(content_qiudui,len(content_qiudui))
                        # print(content_,len(content_))
                        li = []
                        lis = []
                        for p, i in enumerate(content_):
                            if p != 0 and p % 13 == 0:
                                lis.append(li)
                                li = []
                            li.append(i)
                        # print(lis,len(lis))
                        length = len(content_) - 1
                        if len(content_paiming) == len(content_qiudui) == len(lis) == length / 13:
                            for rank, url, content in zip(content_paiming, content_qiudui, lis):
                                teamId = url.split('-')[-1].strip()
                                huang_hongpai = content[-1].strip()
                                if huang_hongpai.isdigit():
                                    yellowCard = int(huang_hongpai)
                                    redCard = ''
                                else:
                                    yellowCard = int(huang_hongpai.split('  (')[0])
                                    redCard = int(huang_hongpai.split('  (')[-1].split(')')[0])

                                item = {'rank': int(rank.strip()), 'teamId': self.get_interger(teamId), 'playedNo': self.get_interger(content[-11].strip()),
                                        'goalFor': self.get_interger(content[-10].strip()), 'goalAgainst': self.get_interger(content[-9].strip()), 'shoot': self.get_interger(content[-8].strip()),
                                        'shootOnTarget': self.get_interger(content[-7].strip()),'penalty': self.get_interger(content[-6].strip()),
                                        'keyPass': self.get_interger(content[-5].strip()),'steal': self.get_interger(content[-4].strip()),
                                        'clearance': self.get_interger(content[-3].strip()),'foul': self.get_interger(content[-2].strip()),
                                        'yellowCard': yellowCard,'redCard': redCard}
                                rank_list.append(item)
                        else:
                            raise Exception('球队数据长度匹配异常！')
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_qd, seasonId, items) #取消注释
                    # print(items)
                    logger.info('seasonId{}:球队数据下载成功'.format(seasonId))

    def parse_data_rangqiu(self, seasonId, leagueId):  # 让球栏数据匹配
        # col_item = {'总盘路':'totalAsiaHandicap','主场盘路':'totalHomeAsiaHandicap','客场盘路':'totalAwayAsiaHandicap',
        #             '半场总盘路':'halfAsiaHandicap','半场主场盘路':'halfHomeAsiaHandicap','半场客场盘路':'halfAwayAsiaHandicap'}
        wait = WebDriverWait(self.driver, 10)
        try:
            rangqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[3]')
            rangqiu_y.click()
        except:
            rangqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[3]')
            rangqiu_y.click()
        # a_list = self.driver.find_elements_by_xpath('//*[@id="concede-points"]/div/a')
        a_list = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="concede-points"]/div/a')))
        if a_list:
            for r, i in enumerate(a_list):
                items = {'id': r + 1, 'leagueId': leagueId, 'seasonId': seasonId }
                rank_list = []
                # time.sleep(0.5)
                i.click()
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="concede-points"]/table/tbody/tr')))
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page, '//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    content_paiming = self.my_xpath(page, '//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/span/text()')
                    content_qiudui = self.my_xpath(page, '//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/@href')
                    content_qiudui_name = self.my_xpath(page, '//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/span/text()')
                    content_ = self.my_xpath(page, '//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]/td/text()')
                    content_.append(88888)
                    # print(content_paiming,len(content_paiming))
                    # print(content_qiudui,len(content_paiming))
                    # print(content_,len(content_))
                    # print(content_qiudui_name,len(content_qiudui_name))
                    li = []
                    lis = []
                    for p,i in enumerate(content_):
                        if p != 0 and p%8 == 0:
                            lis.append(li)
                            li = []
                        li.append(i)
                    # print(lis,len(lis))
                    length = len(content_) - 1
                    if len(content_paiming) == len(content_qiudui) == len(content_qiudui_name) ==  len(lis) == length / 8:
                        for rank,url,qiuduiName,content in zip(content_paiming,content_qiudui,content_qiudui_name,lis):
                            teamId = url.split('-')[-1]
                            qiuduiName = qiuduiName.strip()
                            if teamId:                 #取消注释
                                teamId = int(teamId)
                            else:
                                note = self.col_team.find_one({'name_zh': qiuduiName}, {'teamId': 1})  #取消注释
                                if note:
                                    teamId = note.get('teamId')
                                else:
                                    teamId = qiuduiName
                            item = {'rank':int(rank),'teamId':teamId,'played':self.get_interger(content[0]),'win':self.get_interger(content[1]),
                                    'draw':self.get_interger(content[2]),'lose':self.get_interger(content[3]),'upTeam':self.get_interger(content[5]),
                                    'normal':self.get_interger(content[6]),'downTeam':self.get_interger(content[7])}
                            rank_list.append(item)
                    else:
                        raise Exception('让球栏数据长度匹配异常！')
                items['handicapTable'] = rank_list
                if name == '总盘路':  #取消注释
                    self.save_database(self.collection_rt, seasonId, items)
                elif name == '主场盘路':
                    self.save_database(self.collection_rh, seasonId, items)
                elif name == '客场盘路':
                    self.save_database(self.collection_ra, seasonId, items)
                elif name == '半场总盘路':
                    self.save_database(self.collection_rht, seasonId, items)
                elif name == '半场主场盘路':
                    self.save_database(self.collection_rhh, seasonId, items)
                elif name == '半场客场盘路':
                    self.save_database(self.collection_rha, seasonId, items)
                # print(items)
                logger.info('seasonId{}:让球栏数据匹配{}下载成功'.format(seasonId, name))

    def parse_data_jinqiushu(self, seasonId, leagueId):  # 进球数栏数据匹配
        wait = WebDriverWait(self.driver, 10)
        try:
            jinqiu_y = self.driver.find_element_by_xpath(
                '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[4]')
            jinqiu_y.click()
        except:
            jinqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[4]')
            jinqiu_y.click()
        # a_list = self.driver.find_elements_by_xpath('//*[@id="size-page"]/div/a')
        a_list = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="size-page"]/div/a')))
        if a_list:
            for r, i in enumerate(a_list):
                items = {'id': r + 1, 'leagueId': leagueId, 'seasonId': seasonId, }
                rank_list = []
                # time.sleep(0.5)
                i.click()
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="size-page"]/table/tbody/tr')))
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page, '//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    content_paiming = self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/span/text()')
                    content_qiudui = self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/@href')
                    content_qiudui_name = self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]/td[2]/a/span/text()')
                    content_ = self.my_xpath(page, '//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]/td/text()')
                    content_.append(88888)
                    # print(content_paiming,len(content_paiming))
                    # print(content_qiudui,len(content_paiming))
                    # print(content_,len(content_))
                    # print(content_qiudui_name, len(content_qiudui_name))
                    li = []
                    lis = []
                    for p, i in enumerate(content_):
                        if p != 0 and p % 6 == 0:
                            lis.append(li)
                            li = []
                        li.append(i)
                    # print(lis,len(lis))
                    length = len(content_) - 1
                    if len(content_paiming) == len(content_qiudui) == len(content_qiudui_name) == len(lis) == length / 6:
                        for rank, url,qiuduiName,content in zip(content_paiming, content_qiudui, content_qiudui_name, lis):
                            teamId = url.split('-')[-1].strip()
                            qiuduiName = qiuduiName.strip()
                            if teamId:                 #取消注释
                                teamId = int(teamId)
                            else:
                                note = self.col_team.find_one({'name_zh': qiuduiName}, {'teamId': 1})  #取消注释
                                if note:
                                    teamId = note.get('teamId')
                                else:
                                    teamId = qiuduiName
                            item = {'rank': int(rank), 'teamId': teamId, 'played': self.get_interger(content[0]),
                                    'over': self.get_interger(content[1]), 'draw': self.get_interger(content[2]), 'under': self.get_interger(content[3])}
                            rank_list.append(item)
                    else:
                        raise Exception('进球栏数据长度匹配异常！')
                items['goalLineTable'] = rank_list
                if name == '总盘路':      #取消注释
                    self.save_database(self.collection_jt, seasonId, items)
                elif name == '主场盘路':
                    self.save_database(self.collection_jh, seasonId, items)
                elif name == '客场盘路':
                    self.save_database(self.collection_ja, seasonId, items)
                elif name == '半场总盘路':
                    self.save_database(self.collection_jht, seasonId, items)
                elif name == '半场主场盘路':
                    self.save_database(self.collection_jhh, seasonId, items)
                elif name == '半场客场盘路':
                    self.save_database(self.collection_jha, seasonId, items)
                # print(items)
                logger.info('seasonId{}:进球栏{}下载成功'.format(seasonId, name))

    def parse_data_banquanchang(self, seasonId, leagueId):  # 半全场数据匹配
        wait = WebDriverWait(self.driver, 10)
        try:
            banquanchang_y = self.driver.find_element_by_xpath(
                '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[5]')
            banquanchang_y.click()
        except:
            banquanchang_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[5]')
            banquanchang_y.click()
        # a_list = self.driver.find_elements_by_xpath('//*[@id="double-result"]/div/a')
        a_list = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="double-result"]/div/a')))
        if a_list:
            for p, i in enumerate(a_list):
                items = {'id': p + 1, 'leagueId': leagueId, 'seasonId': seasonId, }
                rank_list = []
                i.click()
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="double-result"]/table/tbody/tr')))
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page, '//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    content_qiudui = self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/a/@href')
                    content_qiudui_name = self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]/td[1]/a/span/text()')
                    content_ = self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]/td/text()')
                    content_.append(88888)
                    # print(content_qiudui,len(content_qiudui))
                    # print(content_,len(content_))
                    # print(content_qiudui_name, len(content_qiudui_name))
                    li = []
                    lis = []
                    for p, i in enumerate(content_):
                        if p != 0 and p % 9 == 0:
                            lis.append(li)
                            li = []
                        li.append(i)
                    # print(lis,len(lis))
                    length = len(content_) - 1
                    if len(content_qiudui) == len(content_qiudui_name) == len(lis) == length / 9:
                        for url, qiuduiName, content in zip(content_qiudui,content_qiudui_name,lis):
                            teamId = url.split('-')[-1]
                            qiuduiName = qiuduiName.strip()
                            if teamId:                 #取消注释
                                teamId = int(teamId)
                            else:
                                note = self.col_team.find_one({'name_zh': qiuduiName}, {'teamId': 1})  #取消注释
                                if note:
                                    teamId = note.get('teamId')
                                else:
                                    teamId = qiuduiName
                            item = {'teamId': teamId, 'ww': self.get_interger(content[0]), 'wd': self.get_interger(content[1]),
                                    'wl': self.get_interger(content[2]), 'dw': self.get_interger(content[3]), 'dd': self.get_interger(content[4]),
                                    'dl': self.get_interger(content[5]), 'lw': self.get_interger(content[6]),'ld':self.get_interger(content[7]),'ll':self.get_interger(content[8])}
                            rank_list.append(item)
                    else:
                        raise Exception('半全场数据长度匹配异常！')
                items['halfFullTable'] = rank_list
                if name == '总计':                                 #取消注释
                    self.save_database(self.collection_bt, seasonId, items)
                elif name == '主场':
                    self.save_database(self.collection_bh, seasonId, items)
                elif name == '客场':
                    self.save_database(self.collection_ba, seasonId, items)
                # print(items)
                logger.info('seasonId{}:半全场{}下载成功'.format(seasonId, name))
    def get_interger(self, data):
        try:
            data = data.strip()
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

    def exit_chrome(self):
        time.sleep(5)
        self.driver.quit()

    def save_file(self, info):  # 保存
        with open('E:\\data\\match.txt', 'a') as f:
            f.write(info)

    def save_database(self, collection, seasonId, items):
        data = collection.find_one({'seasonId': seasonId})
        if not data:
            collection.insert_one(items)
        else:
            collection.update_one({'seasonId': seasonId}, {'$set': items})

    def parse_data(self, note,leagueId):
        seasonId = int(note)
        url = 'https://data.leisu.com/zuqiu-{}'.format(seasonId)
        self.get_data(url)
        # leagueId = '' #去掉
        try:
            # self.parse_data_jifen(seasonId, leagueId)
            self.jifenbang(seasonId, leagueId)
        except Exception as e:
            # self.requests_jifen(seasonId,leagueId)
            self.mdb.sadd('errorSeason', note) #取消注释
            logger.info('{}下载失败！{}'.format(e, note))

        self.parse_data_qiudui(seasonId, leagueId)
        self.parse_data_rangqiu(seasonId, leagueId)
        self.parse_data_jinqiushu(seasonId, leagueId)
        self.parse_data_banquanchang(seasonId, leagueId)

        # self.exit_chrome()

    def run(self,seasonUrl):  #取消注释
        while True:
            print('开始从redis读取。。。')
            note = self.mdb.spop(seasonUrl)
            if note and int(note) != 8796:
                try:
                    leagueId = self.getLeagueId(int(note)) #取消注释
                    if leagueId and (leagueId != 457 and leagueId != 24):
                        self.parse_data(note,leagueId)
                except Exception as e:
                    self.mdb.sadd('errorSeason', note)
                    logger.info('{}下载失败！{}'.format(e, note))
            else:
                print('开始睡眠...')
                time.sleep(60 * 10)
    def get_season_id(self):
        season_note = self.col_read.find({},{'seasonId':1,'leagueId':1})
        for i in season_note:
            seasonId = i.get('seasonId')
            leagueId = i.get('leagueId')
            try:
                if leagueId != 457 and leagueId != 24:
                    self.parse_data(seasonId,leagueId)
            except Exception as e:
                self.mdb.sadd('errorSeason', seasonId)
                logger.info('{}下载失败！{}'.format(e, seasonId))

if __name__ == "__main__":
    a = TiyuData()
    a.run('seasonUrl')
    # a.run('getSeasonUrl')
    # a.get_season_id()
    # a.run('errorSeason')
    # a.parse_data(8453,83)
