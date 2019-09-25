import re
import logging
import chardet
import time
import json
import random
from urllib import parse
import datetime
from threading import Thread
from multiprocessing.pool import ThreadPool
from json.decoder import JSONDecodeError

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


# from datetime import datetime
import time
import pymongo
import redis
from lxml import etree
import requests

from user_agent import agents
from config import config

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class QiuTan:
    def __init__(self):
        self.a = 0
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(executable_path='chromedriver1.exe', chrome_options=chrome_options)
        
        # client = pymongo.MongoClient()
        client_a = pymongo.MongoClient(config)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
        dbnameFootball = client_a['football']
        # dbname = client['odd']
        self.collection_gm = dbnameFootball['generalMatchResult']
        self.collection_sm = dbnameFootball['seniorMatchResult']
        self.col_season = dbnameFootball['season']
        self.col_team = dbnameFootball['team']
        self.collection_com = dbnameFootball['betCompany'] #赔率公司
        
        self.col_aozhi_odd = dbnameFootball['europeOdds'] #欧指赔率数据
        self.col_yazhi_odd = dbnameFootball['handicap'] #亚指赔率数据
        self.col_daxiao_odd = dbnameFootball['goalLine'] #大小赔率数据

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
            if shu <= 20:
                resp = self.getData(url, shu)
                return resp
            else:
                raise Exception('递归次数超过10')
        if resp.status_code == 200:
            return resp
        else:
            shu += 1
            if shu <= 20:
                resp = self.getData(url, shu)
                return resp
            else:
                raise Exception('递归次数超过10!')

    def get_data(self, url):
        resp = self.getData(url, self.a)
        # resp.encoding = chardet.detect(resp.content) #获取网页编码格式
        return resp.text

    def get_data2(self, url):
        resp = self.getData(url, self.a)
        # resp.encoding = chardet.detect(resp.content) #获取网页编码格式
        resp.encoding = 'gb2312'
        return resp.text

    def take_selenium(self, url):
        self.driver.get(url)  # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        time.sleep(1)

    def get_interger(self, data):
        try:
            res = int(data)
        except:
            try:
                res = float(data)
            except:
                res = data
        return res

    def transfer_int(self, li):
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

    def time_stamp(self, m): #转时间戳
        if m:
            ti = datetime.datetime.strptime(m, '%Y-%m-%d %H:%M')
            stamp = round(ti.timestamp())
        else:
            stamp = ''
        return stamp
        
    def transfer_yazhi(self,shuju): #受让
        item = {'平手':0,'半球':0.5,'一球':1,'球半':1.5,'两球':2,
        '两球半':2.5,'三球':3,'三球半':3.5,'四球':4,'四球半':4.5,
        '五球':5,'五球半':5.5,'六球':6,'六球半':6.5,'七球':7,'七球半':7.5,
        '八球':8,'八球半':8.5,'九球':9,'九球半':9.5,'十球':10}
        if shuju[:2] == '受让':
            shu = shuju[2:].split('/')
            if len(shu) == 2:
                a = item.get(shu[0])
                b = item.get(shu[1])
                if a and b:
                    yaz = -(a + b)/2
                else:
                    raise Exception('亚盘数据异常！')
            else:
                yaz = -(item.get(shu[0]))
        else:
            shu = shuju.split('/')
            if len(shu) == 2:
                a = item.get(shu[0])
                b = item.get(shu[1])
                if a and b:
                    yaz = -(a + b)/2
                else:
                    raise Exception('亚盘数据异常！')
            else:
                yaz = -(item.get(shu[0]))
        return yaz
                
        # print(shuju[:2])
        
    def transfer_daxiao(self,shuju):
        if isinstance(shuju,str):
            data = shuju.split('/')
            shuju = (self.get_interger(data[0]) + self.get_interger(data[1]))/2
        return shuju
            
    def get_commanyId(self,name): #获取赔率公司ID
        note = self.collection_com.find_one({'nameZH': {'$regex': name}})
        if note:
            companyId = note.get('companyId')
        else:
            note = self.collection_com.find_one({'nameEN': {'$regex': name}})
            if note:
                companyId = note.get('companyId')
            else:
                companyId = ''
        return companyId
        
    def get_teamId(self,teamNameEn): #获取球队Id 
        note = self.col_team.find_one({'name_en':teamNameEn},{'teamId':1,'leagueId':1})
        if note:
            teamId = note.get('teamId')
        else:
            teamId = ''
        return teamId
        
    def get_teamName(self,home_url,away_url,a=1):
        try:
            result_home = self.get_data(home_url)
            result_away = self.get_data(away_url)
            homeTeamNameEn = self.is_empty(self.my_xpath(result_home,'/html/head/title/text()')).split(' -- ')[0].split(',')[-1].strip()
            awayTeamNameEn = self.is_empty(self.my_xpath(result_away,'/html/head/title/text()')).split(' -- ')[0].split(',')[-1].strip()
        except:
            if a<=5:
                a += 1
                homeTeamNameEn,awayTeamNameEn = self.get_teamName(home_url,away_url,a)
            else:
                homeTeamNameEn = ''
                awayTeamNameEn = ''
        return homeTeamNameEn,awayTeamNameEn
        
    def get_matchId(self,time_s,*args): 
        timestamp = time_s
        leagueName, homeTeamName, awayTeamName, homeScore, awayScore, homeHalfScore, awayHalfScore,home_url,away_url = args
        
        note_le = self.col_season.find_one({'leagueName':leagueName})
        if not note_le:
            note_le = self.col_season.find_one({'leagueFullName':leagueName})
        if note_le:
            leagueFullName = note_le.get('leagueFullName')                                     
            matchId_note = self.collection_gm.find_one({'matchTime': timestamp, 'leagueName': leagueName, 'homeTeamName': homeTeamName,
                                                        'awayTeamName': awayTeamName, 'homeScore': homeScore, 'awayScore': awayScore,
                                                        'homeScoreHalf': homeHalfScore, 'awayScoreHalf': awayHalfScore}, {'matchId': 1})
            if not matchId_note:
                matchId_note = self.collection_gm.find_one({'matchTime':timestamp,'leagueName':leagueFullName,'homeTeamName':homeTeamName,
                                                   'awayTeamName':awayTeamName,'homeScore':homeScore,'awayScore':awayScore,
                                                   'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})    
                                                            
        else:
            matchId_note = self.collection_gm.find_one({'matchTime': timestamp,'homeTeamName': homeTeamName,
                                                        'awayTeamName': awayTeamName, 'homeScore': homeScore, 'awayScore': awayScore,
                                                        'homeScoreHalf': homeHalfScore, 'awayScoreHalf': awayHalfScore},{'matchId':1})
                
        if matchId_note:
            matchId = matchId_note.get('matchId')
        else:
            if note_le:
                leagueFullName = note_le.get('leagueFullName')
                matchId_note = self.collection_sm.find_one(
                    {'matchTime': timestamp, 'leagueName': leagueFullName, 'homeTeamName': homeTeamName,
                     'awayTeamName': awayTeamName, 'homeScore': homeScore, 'awayScore': awayScore,
                     'homeScoreHalf': homeHalfScore, 'awayScoreHalf': awayHalfScore}, {'matchId': 1})
                     
                if not matchId_note:
                    matchId_note = self.collection_sm.find_one(
                        {'matchTime': timestamp, 'leagueName': leagueName, 'homeTeamName': homeTeamName,
                         'awayTeamName': awayTeamName, 'homeScore': homeScore, 'awayScore': awayScore,
                         'homeScoreHalf': homeHalfScore, 'awayScoreHalf': awayHalfScore}, {'matchId': 1})
            else:
                if not matchId_note:
                    matchId_note = self.collection_sm.find_one(
                        {'matchTime': timestamp, 'homeTeamName': homeTeamName, 'awayTeamName': awayTeamName,
                         'homeScore': homeScore, 'awayScore': awayScore, 'homeScoreHalf': homeHalfScore,
                         'awayScoreHalf': awayHalfScore}, {'matchId': 1})
                         
            if not matchId_note:
                homeTeamNameEn,awayTeamNameEn = self.get_teamName(home_url,away_url)
                home_teamId = self.get_teamId(homeTeamNameEn)
                away_teamId = self.get_teamId(awayTeamNameEn)
                print('主队id:{}'.format(home_teamId))
                print('客队id:{}'.format(away_teamId))
                matchId_note = self.collection_gm.find_one({'matchTime':timestamp,'leagueName':leagueFullName,'homeTeamId':home_teamId,
                                                   'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                   'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})
                
                if not matchId_note:                                   
                    matchId_note = self.collection_sm.find_one({'matchTime':timestamp,'leagueName':leagueFullName,'homeTeamId':home_teamId,
                                                       'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                       'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})                                   
                                                   
                if not matchId_note:
                    matchId_note = self.collection_gm.find_one({'matchTime':timestamp,'leagueName':leagueName,'homeTeamId':home_teamId,
                                                       'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                       'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})
                                                       
                if not matchId_note:
                    matchId_note = self.collection_sm.find_one({'matchTime':timestamp,'leagueName':leagueName,'homeTeamId':home_teamId,
                                                   'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                   'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})                                       
                
                if not matchId_note:
                    matchId_note = self.collection_gm.find_one({'matchTime':timestamp,'homeTeamId':home_teamId,
                                                       'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                       'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})                                       
                                                                                             
                if not matchId_note:                                   
                    matchId_note = self.collection_sm.find_one({'matchTime':timestamp,'homeTeamId':home_teamId,
                                                       'awayTeamId':away_teamId,'homeScore':homeScore,'awayScore':awayScore,
                                                       'homeScoreHalf':homeHalfScore,'awayScoreHalf':awayHalfScore},{'matchId':1})                                       
            if matchId_note:
                matchId = matchId_note.get('matchId')    
            else:
                matchId = ''

        return matchId

    def match(self,timer):
        date_year = str(timer)[:4]
        url = 'http://bf.win007.com/football/hg/Over_{}.htm'.format(timer)
        resp = self.get_data2(url)
        tr = self.my_xpath(resp,'//table[@id="table_live"]/tr')
        for i in range(2,len(tr)+1):
            leagueName = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[1]/text()'.format(i)))
            match = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[@class="style1"]/@onclick'.format(i)))
            if not match:
                continue
            # match_str = ','.join(match)
            matchId = self.is_empty(re.findall(r'showgoallist\((\d+)\)',match))
            detail_url = 'http://vip.win007.com/AsianOdds_n.aspx?id={}'.format(matchId)
            result = self.get_data(detail_url)
            home_url =  self.is_empty(self.my_xpath(result,'//div[@class="analyhead"]/div[@class="home"]/a/@href'))
            away_url =  self.is_empty(self.my_xpath(result,'//div[@class="analyhead"]/div[@class="guest"]/a/@href'))
            result_home = self.get_data(home_url)
            result_away = self.get_data(away_url)
            homeTeamNameEn = self.is_empty(self.my_xpath(result_home,'/html/head/title/text()')).split(' -- ')[0].split(',')[-1].strip()
            awayTeamNameEn = self.is_empty(self.my_xpath(result_away,'/html/head/title/text()')).split(' -- ')[0].split(',')[-1].strip()
            
            
            homeTeamName = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[4]/text()'.format(i)))
            homescore = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[5]/font[1]/text()'.format(i)))
            awayscore = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[5]/font[2]/text()'.format(i)))
            awayTeamName = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[6]/text()'.format(i)))
            homeHalfScore = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[7]/font[1]/text()'.format(i)))
            awayHalfScore = self.is_empty(self.my_xpath(resp,'//table[@id="table_live"]/tr[{}]/td[7]/font[2]/text()'.format(i)))
            self.run_match(matchId,date_year,leagueName,homeTeamName,awayTeamName,homescore,awayscore,homeHalfScore,awayHalfScore,homeTeamNameEn,awayTeamNameEn)
            # print(matchId,date_year,leagueName,homeTeamName,awayTeamName,homescore,awayscore,homeHalfScore,awayHalfScore)
        # return matchId
        
    def data_match(self, url='http://zq.win007.com/cn/League/2018-2019/36.html'):  # 球探资料库入口
        self.take_selenium(url)
        wait = WebDriverWait(self.driver, 3)
        try:
            wait.until(EC.presence_of_all_elements_located((By.XPATH, '//table[@id="Table3"]//tr')))
        except:
            try:
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="Table3"]/tbody/tr')))
            except:
                self.take_selenium(url)
        resp = self.driver.page_source
        match_tr = self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr')
        if not match_tr:
            self.take_selenium(url)
            resp = self.driver.page_source
            match_tr = self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr')
            if not match_tr:
                self.take_selenium(url)
                resp = self.driver.page_source
        elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="Table2"]/tbody/tr/td')))
        season_s = self.my_xpath(resp, '//*[@id="seasonList"]/option/text()')
        try:
            leagueName = self.is_empty(self.my_xpath(resp, '//div[@id="TitleLeft"]/text()')).split(' ')[1]
        except:
            leagueName = ''
        for p,element in enumerate(elements):
            element.click()
            resp = self.driver.page_source
            match_tr = self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr')        
            for i in range(3, len(match_tr) + 1):
                matchId = self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/@id'.format(i)))
                homeTeamName = self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[3]/a/text()'.format(i)))
                awayTeamName = self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[5]/a/text()'.format(i)))
                score = self.is_empty(
                    self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[4]/div/a/strong/text()'.format(i)))
                if score:
                    homescore = score.split('-')[0]
                    awayscore = score.split('-')[1]
                else:
                    homescore = 0
                    awayscore = 0
                halfscore = self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[last()]/font/text()'.format(i)))
                if halfscore:
                    homeHalfScore = halfscore.split('-')[0]
                    awayHalfScore = halfscore.split('-')[1]
                else:
                    homeHalfScore = 0
                    awayHalfScore = 0
                home_url = 'http://zq.win007.com' + self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[3]/a/@href'.format(i)))
                away_url = 'http://zq.win007.com' + self.is_empty(self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr[{}]/td[5]/a/@href'.format(i)))

                print(matchId,leagueName,homeTeamName,awayTeamName,homescore,awayscore,homeHalfScore,awayHalfScore,home_url,away_url)
                # self.run_match(matchId,leagueName,homeTeamName,awayTeamName,homescore,awayscore,homeHalfScore,awayHalfScore,home_url,away_url)  # 未完待续
        return season_s
        
    def season_(self,url='http://zq.win007.com/cn/League/36.html'): #联赛url
        self.take_selenium(url)
        wait = WebDriverWait(self.driver, 3)
        try:
            wait.until(EC.presence_of_all_elements_located((By.XPATH, '//table[@id="Table3"]//tr')))
        except:
            try:
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="Table3"]/tbody/tr')))
            except:
                self.take_selenium(url)
        resp = self.driver.page_source
        match_tr = self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr')
        if not match_tr:
            self.take_selenium(url)
            resp = self.driver.page_source
            match_tr = self.my_xpath(resp, '//*[@id="Table3"]/tbody/tr')
            if not match_tr:
                self.take_selenium(url)
                resp = self.driver.page_source
        season_s = self.my_xpath(resp, '//*[@id="seasonList"]/option/text()')
        for i in season_s:
            url_s = 'http://zq.win007.com/cn/League/{}/36.html'.format(i)
            self.data_match(url_s)

    def run_match(self,matchId,*args):
        aozhi = Thread(target=self.get_aozhi,args=(matchId,*args))
        yazhi = Thread(target=self.get_yazhi,args=(matchId,*args))
        daxiao = Thread(target=self.get_daxiao,args=(matchId,*args))
        aozhi.start()
        yazhi.start()
        daxiao.start()
        aozhi.join()
        yazhi.join()
        daxiao.join()
        print('{}--{}: 比赛赔率下载结束'.format(date_year,matchId))
        # self.get_aozhi(matchId, date_year,args)
        # self.get_yazhi(matchId, date_year,args)
        # self.get_daxiao(matchId, date_year,args)

    def aozhi(self,matchId):
        url = 'http://op1.win007.com/oddslist/{}.htm'.format(matchId)
        self.take_selenium(url)
        wait = WebDriverWait(self.driver, 3)
        try:
            wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="header"]/div[@class="analyhead"]/div[3]/div[1]')))
        except:
            try:
                wait.until(EC.presence_of_all_elements_located((By.XPATH, '/html/body/div[2]/div[1]/div[3]/div[1]/text()')))
            except:
                pass
        resp = self.driver.page_source
        ao_url = self.my_xpath(resp,'//table[@id="oddsList_tab"]/tbody/tr/td[3]/@onclick')
        aozhi_url = ['http://op1.win007.com'+ re.findall(r"OddsHistory\('(.*?Company=).*?'\)",i)[0]
                     + parse.quote(re.findall(r"OddsHistory\('.*?Company=(.*?)'\)",i)[0],encoding='gbk')   for i in ao_url]
        bocai =[i.strip().split('(')[0] for i in self.my_xpath(resp,'//table[@id="oddsList_tab"]/tbody/tr/td[2]/a/text()')]
        a = '/html/body/div[2]/div[1]/div[3]/div[1]/text()'
        timer = self.my_xpath(resp, '//div[@class="header"]/div[@class="analyhead"]/div[3]/div[1]/text()')
        if not timer:
            timer = self.my_xpath(resp, a)
        time_str = ''.join(timer)
        t_r = self.is_empty(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_str))
        if len(aozhi_url) == len(bocai):
            # print(bocai)
            return t_r,zip(bocai,aozhi_url)
        else:
            raise Exception('欧指长度匹配异常！')
        # print(aozhi_url)
        # print(len(aozhi_url))

    def get_aozhi(self,matchId,*args):
        time_str,url_li = self.aozhi(matchId)
        timestamp_a = self.time_stamp(time_str)
        year = time_str.split(' ')[0]
        lei_matchId = self.get_matchId(timestamp_a,*args)
        if lei_matchId:
            for name,url in url_li:
                if name[:1] == '明':
                    name = '明升'
                try:
                    url = url.split('&r1=')[0] + '&Company=' + url.split('&Company=')[-1]
                    resp = self.get_data2(url)
                    tr = self.my_xpath(resp,'//*[@id="odds"]/table/tr')
                    if len(tr) >= 2:
                        companyId = self.get_commanyId(name)  #查库获得
                        if not companyId:
                            continue
                        finsh_h = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[2]/td[1]/b/font/text()'))
                        finsh_z = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[2]/td[2]/b/font/text()'))
                        finsh_a = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[2]/td[3]/b/font/text()'))
                        finsh_time = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[2]/td[last()]/text()'))
                        finsh_date = self.time_stamp(year + '-' + finsh_time)
                        # print(finsh_h, finsh_z, finsh_a, finsh_date)
                        init_h = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[last()]/td[1]/b/font/text()'))
                        init_z = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[last()]/td[2]/b/font/text()'))
                        init_a = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[last()]/td[3]/b/font/text()'))
                        init_time = self.is_empty(self.my_xpath(resp,'//*[@id="odds"]/table/tr[last()]/td[last()]/text()'))
                        init_date = self.time_stamp(year + '-' + init_time)
                        # qiutan = 1 #加个字段，区别爬取的数据和接口过来的数据
                        # print(init_h, init_z, init_a, init_date)
                        item = {
                            'matchId':lei_matchId,
                            'companyId':companyId,
                            'initHomeTeamWinOdds':init_h,
                            'initDrawOdds':init_z,
                            'initAwayTeamWinOdds':init_a,
                            'initTime':init_date,
                            'finalHomeTeamWinOdds':finsh_h,
                            'finalDrawOdds':finsh_z,
                            'finalAwayTeamWinOdds':finsh_a,
                            'finalTime':finsh_date,
                            'qiutan': 1
                        }
                        print(item)
                        note = self.col_aozhi_odd.find_one({'matchId':lei_matchId,'companyId':companyId})
                        if not note:
                            self.col_aozhi_odd.insert_one(item)
                            logger.info('{}-欧指-{}:数据插入成功！'.format(timestamp_a,lei_matchId))
                except Exception as e:
                    print('欧指comany异常:{}'.format(e))
        else:
            print('matchId没有匹配到！')

    def yazhi(self,matchId):
        url = 'http://vip.win007.com/AsianOdds_n.aspx?id={}&l=1'.format(matchId)
        resp = self.get_data(url)
        tr_url = self.my_xpath(resp,'//*[@id="odds"]/tr[not(@class)]/td[last()]/a[1]/@href')
        yazhi_url = ['http://vip.win007.com'+ i for i in tr_url]

        bocai =  self.my_xpath(resp, '//*[@id="odds"]/tr[not(@class)]/td[1]/text()')
        time_str = ''.join(self.my_xpath(resp, '//div[@class="header"]/div[@class="analyhead"]/div[3]/div[1]/text()'))
        t_r = self.is_empty(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_str))
        if len(yazhi_url) == len(bocai):
            return t_r,zip(bocai,yazhi_url)
        else:
            raise Exception('亚指长度匹配异常！')
        # print(yazhi_url)
        # print(len(yazhi_url))

    def get_yazhi(self,matchId,*args):
        time_str,url_li = self.yazhi(matchId)
        timestamp_a = self.time_stamp(time_str)
        year = time_str.split(' ')[0]
        lei_matchId = self.get_matchId(timestamp_a,*args)
        if lei_matchId:
            for name,url in url_li:
                if name[:1] == '明':
                    name = '明升'
                try:
                    resp = self.get_data(url)
                    companyId = self.get_commanyId(name)
                    if not companyId:
                        continue
                    finish_1 = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[1]/font/b/text()'))
                    finish_time = self.is_empty(
                        self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[last()-1]/text()'))
                    finish_date = self.time_stamp(year + '-' + finish_time)
                    if not finish_1:
                        finish_h = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[3]/font/b/text()'))
                        finish_z = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[4]/font/text()'))
                        finish_a = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[5]/font/b/text()'))

                        init_h = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[3]/font/b/text()'))
                        init_z = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[4]/font/text()'))
                        init_a = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[5]/font/b/text()'))
                    else:
                        finish_h = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[1]/font/b/text()'))
                        finish_z = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[2]/font/text()'))
                        finish_a = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[3]/font/b/text()'))

                        init_h = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[1]/font/b/text()'))
                        init_z = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[2]/font/text()'))
                        init_a = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[3]/font/b/text()'))
                    # print(finish_h, finish_z, finish_a, finish_date)
                    init_time = self.is_empty(
                        self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[last()-1]/text()'))
                    init_date = self.time_stamp(year + '-' + init_time)
                    qiutan = 1  # 加个字段，区别爬取的数据和接口过来的数据
                    # print(init_h, init_z, init_a, init_date)
                    try:
                        init_yaz = self.transfer_yazhi(init_z)
                        finish_yaz = self.transfer_yazhi(finish_z)
                    except Exception as e:
                        print('亚盘数据获取异常！')
                        continue
                    item = {
                        "matchId": lei_matchId,
                        "companyId": companyId,
                        "initHomeTeamOdds": init_h,
                        "initAsiaHandicap": init_yaz,
                        "initAwayTeamOdds": init_a,
                        "initHandicapTime": init_date,
                        "finalHomeTeamOdds": finish_h,
                        "finaAsiaHandicap": finish_yaz,
                        "finalAwayTeamOdds": finish_a,
                        "finalHandicapTime": finish_date,
                        "qiutan":1,
                    }
                    print(item)
                    note = self.col_yazhi_odd.find_one({'matchId': lei_matchId, 'companyId': companyId})
                    if not note:
                        self.col_yazhi_odd.insert_one(item)
                        logger.info('{}-亚指-{}:数据插入成功！'.format(timestamp_a,lei_matchId))
                except Exception as e:
                    print('亚指comany异常:{}'.format(e))
        else:
            print('matchId没有匹配到！')
            
    def daxiao(self,matchId):
        url = 'http://vip.win007.com/OverDown_n.aspx?id={}&l=1'.format(matchId)
        resp = self.get_data(url)
        tr_url = self.my_xpath(resp, '//*[@id="odds"]/tr[not(@class)]/td[last()]/a[1]/@href')
        daxiao_url = ['http://vip.win007.com' + i for i in tr_url]
        bocai = self.my_xpath(resp, '//*[@id="odds"]/tr[not(@class)]/td[1]/text()')
        time_str = ''.join(self.my_xpath(resp, '//div[@class="header"]/div[@class="analyhead"]/div[3]/div[1]/text()'))
        t_r = self.is_empty(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_str))
        if len(daxiao_url) == len(bocai):
            return t_r,zip(bocai,daxiao_url)
        else:
            raise Exception('大小长度匹配异常！')
        # print(daxiao_url)
        # print(len(daxiao_url))

    def get_daxiao(self,matchId,*args):
        time_str,url_li = self.daxiao(matchId)
        timestamp_a = self.time_stamp(time_str)
        year = time_str.split(' ')[0]
        lei_matchId = self.get_matchId(timestamp_a,*args)
        if lei_matchId:
            for name,url in url_li:
                if name[:1] == '明':
                    name = '明升'
                try:
                    resp = self.get_data(url)
                    companyId = self.get_commanyId(name)
                    if not companyId:
                        continue
                    finish_1 = self.is_empty(
                        self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[1]/font/b/text()'))
                    finish_time = self.is_empty(
                        self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[last()-1]/text()'))
                    finish_date = self.time_stamp(year + '-' + finish_time)
                    if not finish_1:
                        finish_h = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[3]/font/b/text()'))
                        finish_z = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[4]/font/text()'))
                        finish_a = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[5]/font/b/text()'))

                        init_h = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[3]/font/b/text()'))
                        init_z = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[4]/font/text()'))
                        init_a = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[5]/font/b/text()'))
                    else:
                        finish_h = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[1]/font/b/text()'))
                        finish_z = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[2]/font/text()'))
                        finish_a = self.is_empty(
                            self.my_xpath(resp, '//*[@id="odds2"]/table/tr[@bgcolor="#ffffff"]/td[3]/font/b/text()'))

                        init_h = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[1]/font/b/text()'))
                        init_z = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[2]/font/text()'))
                        init_a = self.is_empty(self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[3]/font/b/text()'))
                    init_time = self.is_empty(
                        self.my_xpath(resp, '//*[@id="odds2"]/table/tr[last()]/td[last()-1]/text()'))
                    init_date = self.time_stamp(year + '-' + init_time)
                    # qiutan = 1  # 加个字段，区别爬取的数据和接口过来的数据
                    # print(init_h, init_z, init_a, init_date)
                    item = {
                        "matchId": lei_matchId,
                        "companyId": companyId,
                        "initHomeTeamOdds": init_h,
                        "initGoalLine": self.transfer_daxiao(init_z),
                        "initAwayTeamOdds": init_a,
                        "initTime": init_date,
                        "finalHomeTeamOdds": finish_h,
                        "finaGoalLine": self.transfer_daxiao(finish_z),
                        "finalAwayTeamOdds": finish_a,
                        "finalTime": finish_date,
                        "qiutan": 1,
                    }
                    print(item)
                    note = self.col_daxiao_odd.find_one({'matchId': lei_matchId, 'companyId': companyId})
                    if not note:
                        self.col_daxiao_odd.insert_one(item)
                        logger.info('{}-大小球-{}:数据插入成功！'.format(timestamp_a,lei_matchId))
                except Exception as e:
                    print('大小comany异常:{}'.format(e))
        else:
            print('matchId没有匹配到！')   
            
    def read_redis(self):
        while True:
            date_start = self.mdb.spop('qiutanTime')
            print('读取缓冲-{}！'.format(date_start))
            if date_start:
                try:
                    self.match(date_start)
                except Exception as e:
                    self.mdb.sadd('qiutanTime',date_start)
                    print('发生错误:{}，将继续重新匹配！'.format(e))
            else:
                print('爬取结束！')
                break
                
    def take_match(self):
        now = datetime.datetime.now().date()
        date_now = now.strftime('%Y%m%d')
        date_start = '20140715'
        while True:
            # date_start = self.mdb.spop('qiutanTime')
            if date_now != date_start:
                try:
                    self.mdb.sadd('qiutanTime',date_start)
                    # self.match(date_start)
                    # print(date_start)
                    date2 = datetime.datetime.strptime(date_start, '%Y%m%d').date()
                    date1 = date2 + datetime.timedelta(days=1)
                    date_start = date1.strftime('%Y%m%d')
                    # url = 'https://live.leisu.com/wanchang?date={}'.format(date_start)
                except Exception as e:
                    self.mdb.sadd('qiutanTime',date_start)
                    print('发生错误:{}，将继续重新匹配！'.format(e))
            else:
                print('添加结束！')
                break
                
if __name__ == '__main__':
    a = QiuTan()
    # a.take_match()
    a.read_redis()
    # a.match('20140715')
    
    # a.tranfer_yazhi('受让一球/球半')  
    # a.match(20190831)
    # a.get_aozhi(1784889,'2019')
    # a.get_yazhi(1784889,'2019')
    # a.get_daxiao(1784889,'2019')
    # a.aozhi(1784889)
    # a.yazhi(1786629)
    # a.daxiao(1786629)
