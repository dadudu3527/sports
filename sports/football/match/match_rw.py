import re
from lxml import etree
import time
import json
import requests
from datetime import datetime
from multiprocessing.pool import ThreadPool
import pymongo
import random
import redis

from user_agent import agents
from config import config
import logging
from json.decoder import JSONDecodeError

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


class Match:
    def __init__(self):
        self.a = 0
        client = pymongo.MongoClient(config)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
        dbnameFootball = client['football']
        self.errorCol = dbnameFootball['errorMatch']
        self.col_read = dbnameFootball['season']
        self.collection_league = dbnameFootball['league']  # 读取level所创建集合
        self.collection_2 = dbnameFootball['seniorSchedule']  # 高级赛程集合
        self.collection_3 = dbnameFootball['generalSchedule']  # 普通赛程集合
        self.collection_4 = dbnameFootball['seniorMatchResult']  # 高级赛果集合
        self.collection_5 = dbnameFootball['generalMatchResult']  # 普通赛果集合
        self.pool = ThreadPool(20)

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
        # headers = {'User-Agent': random.choice(agents)}
        # resp = requests.get(url=url, headers=headers)
        # if resp.status_code == 200:
            # pass
        # else:
        resp = self.getData(url,self.a)
        return resp.text

    def parse_data_saiguo(self, seasonId):  # 赛程赛果
        try:
            logger.info('{}数据下载中'.format(seasonId))
            note = self.col_read.find_one({'seasonId': seasonId})
            if note:
                leagueId = note.get('leagueId')
            else:
                leagueId = ''
            page = self.get_data('https://data.leisu.com/zuqiu-{}'.format(seasonId))
            tr_length = self.my_xpath(page, '//div[@id="matches"]/table//tr')
            leagueName = self.is_empty(self.my_xpath(page,
                                                     '//div[@class="display-b p-r-30 p-l-30"]//div[@class="clearfix-row f-s-24"]/text()'))
            seasonYear = self.is_empty(self.my_xpath(page, '//div[@class="select-border"]/span/span/text()'))
            if tr_length:
                for i in range(1, len(tr_length)):
                    matchId = self.is_empty(
                        self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-id'.format(i + 1)))
                    if matchId:
                        items = {}
                        # matchTime = self.my_xpath(page,
                                                  # '//*[@id="matches"]/table//tr[{}]/td[2]/span/text()'.format(i + 1))
                        # matchTime = ' '.join(matchTime)
                        homeTeamId = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-home-id'.format(i + 1)))
                        homeTeamName = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/td[3]/a/text()'.format(i + 1)))
                        homeTeamRank = self.is_empty(self.my_xpath(page,
                                                                   '//*[@id="matches"]/table//tr[{}]/td[3]/a/span/text()'.format(
                                                                       i + 1))).replace('[', '').replace(']', '')

                        awayTeamId = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-away-id'.format(i + 1)))
                        awayTeamName = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/td[5]/a/text()'.format(i + 1)))
                        awayTeamRank = self.is_empty(self.my_xpath(page,
                                                                   '//*[@id="matches"]/table//tr[{}]/td[5]/a/span/text()'.format(
                                                                       i + 1))).replace('[', '').replace(']', '')
                        statusId = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-status'.format(i + 1)))
                        Mode = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-mode'.format(i + 1)))
                        round = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-round'.format(i + 1)))
                        StageId = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-stage'.format(i + 1)))
                        Group = self.is_empty(
                            self.my_xpath(page, '//*[@id="matches"]/table//tr[{}]/@data-group'.format(i + 1)))
                        stageName = self.is_empty(self.my_xpath(page, '//a[@data-id="{}"]/text()'.format(StageId)))

                        homeData = self.is_empty(self.my_xpath(page,
                                                               '//*[@id="matches"]/table//tr[{}]/@data-home-score'.format(
                                                                   i + 1))).split(',')
                        awayData = self.is_empty(self.my_xpath(page,
                                                               '//*[@id="matches"]/table//tr[{}]/@data-away-score'.format(
                                                                   i + 1))).split(',')

                        homeScore = self.get_interger(homeData[0])
                        awayScore = self.get_interger(awayData[0])

                        items['matchId'] = matchId
                        items['leagueId'] = leagueId
                        items['seasonId'] = seasonId
                        items['statusId'] = statusId
                        items['stageId'] = StageId
                        items['mode'] = Mode
                        items['round'] = round
                        items['group'] = Group
                        items['stageName'] = stageName
                        items['seasonYear'] = seasonYear
                        items['matchTime'] = self.parse_time_stamp(matchId)
                        items['leagueName'] = leagueName
                        items['homeTeamId'] = homeTeamId
                        items['homeTeamName'] = homeTeamName
                        items['awayTeamId'] = awayTeamId
                        items['awayTeamName'] = awayTeamName
                        items['homeTeamRank'] = self.get_interger(homeTeamRank)
                        items['awayTeamRank'] = self.get_interger(awayTeamRank)
                        items['homeScore'] = homeScore
                        items['awayScore'] = awayScore
                        items['homeScoreHalf'] = self.get_interger(homeData[1])
                        items['awayScoreHalf'] = self.get_interger(awayData[1])
                        items['homeRedCard'] = self.get_interger(homeData[2])
                        items['awayRedCard'] = self.get_interger(awayData[2])
                        items['homeYellowCard'] = self.get_interger(homeData[3])
                        items['awayYellowCard'] = self.get_interger(awayData[3])
                        items['homeCorner'] = self.get_interger(homeData[4])
                        items['awayCorner'] = self.get_interger(awayData[4])
                        try:
                            matchdetail = self.increase(matchId, homeScore, awayScore)
                        except Exception as e:
                            self.errorCol.insert_one({'seasonId':seasonId,'matchId':matchId})
                            print('matchId:{}保存失败！{}'.format(matchId,e))
                            continue
                        for name,value in matchdetail.items():
                            items[name] = value

                        #需要一个访问数据库的接口，通过leagueid获取level参数
                        levelNote = self.collection_league.find_one({'leagueId': leagueId})
                        if levelNote:
                            level = levelNote.get('level')
                        else:
                            raise Exception('没有获取到level值！')

                        if statusId == 8:
                            if level <= 2:
                                note = self.collection_2.find_one({'matchId':matchId})
                                if note:
                                    self.collection_2.update_one({'matchId':matchId},{'$set':{'delete':1}})
                                data = self.collection_4.find_one({'matchId': matchId})
                                if not data:
                                    self.collection_4.insert_one(items)
                                else:
                                    self.collection_4.update_one({'matchId': matchId}, {'$set': items})
                            else:
                                note = self.collection_3.find_one({'matchId':matchId})
                                if note:
                                    self.collection_3.update_one({'matchId':matchId},{'$set':{'delete':1}})
                                data = self.collection_5.find_one({'matchId': matchId})
                                if not data:
                                    self.collection_5.insert_one(items)
                                else:
                                    self.collection_5.update_one({'matchId': matchId}, {'$set': items})
                        else:
                            items['delete'] = 0
                            if level <= 2:
                                data = self.collection_2.find_one({'matchId': matchId})
                                if not data:
                                    self.collection_2.insert_one(items)
                                else:
                                    self.collection_2.update_one({'matchId': matchId}, {'$set': items})
                            else:
                                data = self.collection_3.find_one({'matchId': matchId})
                                if not data:
                                    self.collection_3.insert_one(items)
                                else:
                                    self.collection_3.update_one({'matchId': matchId}, {'$set': items})
                        # print(items)
                else:
                    logger.info('{}matchId不存在'.format(seasonId))
            else:
                logger.info('{}没有数据'.format(seasonId))
            logger.info('{}数据下载结束'.format(seasonId))
        except Exception as e:
            ad = self.errorCol.find_one({'seasonId': seasonId})
            if not ad:
                self.errorCol.insert_one({'seasonId': seasonId})
            logger.info('{}数据下载失败！{}'.format(seasonId, e))

    def get_interger(self, data):
        try:
            res = int(data)
        except:
            try:
                res = float(data)
            except:
                res = data
        return res

    def requests_api(self,scheduleid):
        url_2 = 'http://api.leisu.com/api/odds/detail?sid={}'.format(scheduleid)

        # response_1 = json.loads(self.getData(url_1))
        response = self.getData(url_2,self.a)
        try:
            response_2 = json.loads(response.text)
        except JSONDecodeError:
            response_2 = self.requests_api(scheduleid)
        return  response_2
    
    def time_1(self,m):
        ti = datetime.strptime(m, '%Y/%m/%d %H:%M')
        stamp = ti.timestamp()
        return round(stamp)
    
    def get_time(self,scheduleid,a):
        shu = a
        url = 'https://live.leisu.com/3in1-{}'.format(scheduleid)
        resp = self.getData(url,self.a)
        time_str = self.my_xpath(resp.text,'//div[@class="team-center"]/div[1]/text()')
        if not time_str:
            shu += 1
            if shu <= 10:
                time_str = self.get_time(scheduleid,shu)
            else:
                raise Exception('获取时间字符串失败！')
        return time_str
                
    def parse_time_stamp(self,scheduleid):
        time_str = self.get_time(scheduleid,self.a)
        time_str_p = ''.join(time_str).strip().split(' ',1)[-1]
        return self.time_1(time_str_p)
            
    def increase(self, scheduleid, homescore, awayscore):  # 增加每条比赛的，进球数，让球等字段
        item = {}
        if homescore > awayscore:
            item['matchResult'] = 1
        elif homescore < awayscore:
            item['matchResult'] = -1
        else:
            item['matchResult'] = 0
        # url_1 = 'http://api.leisu.com/app/live/matchdetail/?sid={}'.format(scheduleid)
        response_2 = self.requests_api(scheduleid)
        ite_list = response_2.get('data')
        if ite_list:
            for p, m in enumerate(ite_list):
                if m:
                    try:
                        homeOddsj = m[0][0][0]
                        concede_points = str(m[0][0][1])  # 让球
                        awayOddsj = m[0][0][2]
                    except Exception:
                        homeOddsj = 'null'
                        concede_points = 'null'
                        awayOddsj = 'null'
                    try:
                        homeOddsr = m[2][0][0]
                        goals_for = str(m[2][0][1])  # 进球数
                        awayOddsr = m[2][0][2]
                    except Exception:
                        homeOddsr = 'null'
                        goals_for = 'null'
                        awayOddsr = 'null'
                else:
                    homeOddsj = 'null'
                    awayOddsj = 'null'
                    concede_points = 'null'
                    homeOddsr = 'null'
                    awayOddsr = 'null'
                    goals_for = 'null'

                if concede_points.isalpha():
                    pan_road = ''
                else:
                    if homescore - awayscore > float(concede_points):
                        pan_road = 1  # 赢盘
                    elif homescore - awayscore < float(concede_points):
                        pan_road = -1  # 输盘
                    else:
                        pan_road = 0  # 走盘

                if goals_for.isalpha():
                    large_ball = ''
                else:
                    if homescore + awayscore > float(goals_for):
                        large_ball = 1  # 大球
                    elif homescore + awayscore < float(goals_for):
                        large_ball = -1  # 小球
                    else:
                        large_ball = 0  #

                if p == 0:
                    item['initHandicap'] = {'homeOdds': homeOddsj, 'handicap': concede_points, 'awayOdds': awayOddsj}
                    item['initHandicapResult'] = pan_road
                    item['initGoalLine'] = {'homeOdds': homeOddsr, 'goalLine': goals_for, 'awayOdds': awayOddsr}
                    item['initGoalLineResult'] = large_ball
                elif p == 1:
                    item['finishHandicap'] = {'homeOdds': homeOddsj, 'handicap': concede_points, 'awayOdds': awayOddsj}
                    item['finishHandicapResult'] = pan_road
                    item['finishGoalLine'] = {'homeOdds': homeOddsr, 'goalLine': goals_for, 'awayOdds': awayOddsr}
                    item['finishGoalLineResult'] = large_ball
                else:
                    break
        else:
            item['initHandicap'] = {'homeOdds': 'null', 'handicap': 'null', 'awayOdds': 'null'}
            item['initHandicapResult'] = ''
            item['initGoalLine'] = {'homeOdds': 'null', 'goalLine': 'null', 'awayOdds': 'null'}
            item['initGoalLineResult'] = ''
            item['finishHandicap'] = {'homeOdds': 'null', 'handicap': 'null', 'awayOdds': 'null'}
            item['finishHandicapResult'] = ''
            item['finishGoalLine'] = {'homeOdds': 'null', 'goalLine': 'null', 'awayOdds': 'null'}
            item['finishGoalLineResult'] = ''
        return item

    def my_xpath(self, data, ruler):
        return etree.HTML(data).xpath(ruler)

    def is_empty(self, data):  # 处理数据为空的字段 str转int float
        if data != [] and isinstance(data, list):
            jieguo = data[0].strip()
            if jieguo.isdigit():
                jieguo = int(jieguo)
            else:
                if not jieguo.isalpha():
                    try:
                        jieguo = float(jieguo)
                    except:
                        pass
        elif isinstance(data, str):
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

    # def add_match_field(self):
    # 查询match数据库
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
                for li in range(1,len(ul)+1):
                    seasonId = response.xpath('//div[@class="left-list"]/div[1]/div[3]/ul/li[{}]/@data-season-id'.format(li))[0]
                    seasonId_liu.append(int(seasonId))
            else:
                for r in range(2,len(children_div)+1):
                    # name_country = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/div[1]/span[@class="txt"]/text()'.format(i,r))
                    ul = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/ul/li'.format(i,r))
                    for li in range(1,len(ul)+1):
                        seasonId = response.xpath('//div[@class="left-list"]/div[{}]/div[{}]/ul/li[{}]/@data-season-id'.format(i,r,li))[0]
                        seasonId_li.append(int(seasonId))
        tup = (seasonId_li,seasonId_liu)
        if not seasonId_li and not seasonId_liu:
            tup = self.current_season()
        return tup
    
    def thread_pool2(self):   
        while True:
            print('开始从redis读取。。。')
            note = self.mdb.spop('SeasonMatch')
            if note:
                try:
                    self.parse_data_saiguo(int(note))
                except Exception as e:
                    self.mdb.sadd('errorSeasonMatch',note)
                    logger.info('{}下载失败！{}'.format(e, note))
            else:
                print('开始睡眠...')
                time.sleep(60*5)
    def get_current_season(self):
        li,liu = self.current_season()
        tup = tuple(li)
        self.mdb.sadd('renewCurrentSeason',*tup)
        
   
if __name__ == '__main__':
    a = Match()  # 'https://data.leisu.com/team-40000'
    # a.thread_pool()
    # time.sleep(60)
    # a.parse_data_saiguo(9168)
    a.thread_pool2()
    # a.get_current_season()
