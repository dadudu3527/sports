import re
import time
import json
from datetime import datetime
import time
from multiprocessing.pool import ThreadPool
import random
import logging
from json.decoder import JSONDecodeError

import requests
from lxml import etree
import pymongo

from user_agent import agents
from config import config

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


class Match:
    def __init__(self):
        self.a = 0
        client = pymongo.MongoClient(config)
        # client_a = pymongo.MongoClient()
        dbnameFootball = client['football']
        # dbname = client_a['match']
        # self.errorCol = dbnameFootball['errorMatch']
        self.col_read = dbnameFootball['season']
        # self.collection_league = dbnameFootball['league']  # 读取level所创建集合
        self.collection_2 = dbnameFootball['seniorSchedule']  # 高级赛程集合
        self.collection_3 = dbnameFootball['generalSchedule']  # 普通赛程集合
        self.collection_4 = dbnameFootball['seniorMatchResult']  # 高级赛果集合
        self.collection_5 = dbnameFootball['generalMatchResult']  # 普通赛果集合
        self.col_team = dbnameFootball['team']
        self.pool = ThreadPool(20)

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
        
    def transfer_int(self,li):
        lis = []
        for i in li:
            lis.append(self.get_interger(i))
        return lis

    def requests_api(self, scheduleid):
        url_2 = 'http://api.leisu.com/api/odds/detail?sid={}'.format(scheduleid)

        # response_1 = json.loads(self.getData(url_1))
        response = self.getData(url_2, self.a)
        try:
            response_2 = json.loads(response.text)
        except JSONDecodeError:
            response_2 = self.requests_api(scheduleid)
        return response_2

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
        if isinstance(data, list):
            if data != []:
                jieguo = data[0].strip()
                jieguo = self.get_interger(jieguo)
            else:
                jieguo = ''
        else:
            jieguo = self.get_interger(data)
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
        for i in range(1, 8):
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
                        seasonId = response.xpath(
                            '//div[@class="left-list"]/div[{}]/div[{}]/ul/li[{}]/@data-season-id'.format(i, r, li))[0]
                        seasonId_li.append(int(seasonId))
        tup = (seasonId_li, seasonId_liu)
        if not seasonId_li and not seasonId_liu:
            tup = self.current_season()
        return tup

    def renewMatch(self,match_url='https://live.leisu.com/wanchang'):
        date = match_url.split('=')[-1]
        url = match_url
        resp = self.get_data(url)
        li = self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li')
        item = {'胜':1,'负':-1,'平':0}
        for i in range(1,len(li)+1):
            matchId = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-id'.format(i)))
            leagueId = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-eventid'.format(i)))
            homeTeamUrl = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]/span[1]/a/@href'.format(i)))
            homeTeamName = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]/span[1]/a/text()'.format(i)))
            homeTeamId = self.get_interger(homeTeamUrl.split('-')[-1])  
            awayTeamUrl = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]/span[1]/a/@href'.format(i)))
            awayTeamName = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]/span[1]/a/text()'.format(i)))
            awayTeamId = self.get_interger(awayTeamUrl.split('-')[-1])
            
            # #更新球队所属联赛
            # home_note = self.col_team.find_one({'teamId':homeTeamId},{'leagueId':1})
            # if home_note:
                # lgId = home_note.get('leagueId')
                # if leagueId != lgId:
                    # self.col_team.update_one({'teamId':homeTeamId},{'$set':{'leagueId':leagueId}})
            # away_note = self.col_team.find_one({'teamId':awayTeamId},{'leagueId':1})
            # if away_note:
                # lgId = away_note.get('leagueId')
                # if leagueId != lgId:
                    # self.col_team.update_one({'teamId':awayTeamId},{'$set':{'leagueId':leagueId}})
            # #更新球队所属联赛
            
            season_url = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="lab-events"]/a/@href'.format(i)))
            if isinstance(matchId,str):
                continue

            finishHandicap = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-asian'.format(i)))
            finishHandicap_list = finishHandicap.split(',')
            if len(finishHandicap_list) != 4:
                finishHandicap_list = ['null','null','null']
            else:
                finishHandicap_list = self.transfer_int(finishHandicap_list)
            finishGoalLine = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-daxiao'.format(i)))
            finishGoalLine_list = finishGoalLine.split(',')
            if len(finishGoalLine_list) != 4:
                finishGoalLine_list = ['null','null','null']
            else:
                finishGoalLine_list = self.transfer_int(finishGoalLine_list)
            
            matchTime = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-nowtime'.format(i)))
            statusId = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-status'.format(i)))
            
            homeRank = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]/span[1]/span[1]/text()'.format(i)))
            homeRank = self.is_empty(re.findall(r'\[.*?(\d+).*?\]',homeRank))

            homeYellowCard = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]/span[1]/span[2]/text()'.format(i)))
            homeRedCard = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]/span[1]/span[3]/text()'.format(i)))

            allScore = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[2]/span[1]/b/text()'.format(i)))
            homeScore = int(allScore.split('-')[0])
            awayScore = int(allScore.split('-')[1])

            awayRedCard = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]/span[1]/span[1]/text()'.format(i)))
            awayYellowCard = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]/span[1]/span[2]/text()'.format(i)))

            awayRank = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]/span[1]/span[3]/text()'.format(i)))
            awayRank = self.is_empty(re.findall(r'\[.*?(\d+).*?\]', awayRank))

            halfScore = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-right"]/span[@class="lab-half"]/text()'.format(i)))
            homeHalfScore = int(halfScore.split('-')[0])
            awayHalfScore = int(halfScore.split('-')[1])

            corner = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-right"]/span[@class="lab-corner"]/span[1]/text()'.format(i)))
            if corner:
                homeCorner = int(corner.split('-')[0])
                awayCorner = int(corner.split('-')[1])
            else:
                homeCorner = 0
                awayCorner = 0
            result = self.is_empty(self.my_xpath(resp,
                '//ul[@class="layout-grid-list"]/li[{}]/div[1]/div[1]/span[@class="float-right"]/span[@class="lab-bet-odds"]/span[1]/text()'.format(i))).strip()

            # print(matchId,finishHandicap_list,finishGoalLine_list,matchTime,statusId,season_url,homeRank,homeYellowCard,homeRedCard,allScore,awayRank,awayYellowCard,awayRedCard,halfScore,corner,result)
            # print(homeScore,awayScore,'---',homeHalfScore,awayHalfScore,'---',homeCorner,awayCorner)
            note_senior = self.collection_2.find_one({'matchId': matchId},{'_id':0})
            note = self.collection_3.find_one({'matchId': matchId},{'_id':0})
            if not note_senior and not note:
                #添加比赛字段    
                level = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/@data-eventlevels'.format(i)))
                round = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div/div/span[2]/text()'.format(i)))
                season_url = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div/div/span[1]/a/@href'.format(i)))
                if season_url == 'javascript:void(0)':
                    seasonId = ''
                    seasonYear = ''
                    leagueName = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div/div/span[1]/a/span/text()'.format(i)))
                else:
                    seasonId = self.get_interger(season_url.split('-')[-1].replace('/',''))
                    season_note = self.col_read.find_one({'seasonId':seasonId})
                    if season_note:
                        leagueName = season_note.get('leagueFullName')
                        seasonYear = season_note.get('seasonYear')
                    else:
                        leagueName = self.is_empty(self.my_xpath(resp,'//ul[@class="layout-grid-list"]/li[{}]/div/div/span[1]/a/span/text()'.format(i)))
                        seasonYear = ''
                #添加比赛字段
                item_match = {
                    'matchId':matchId,
                    'leagueId':leagueId,
                    'seasonId':'',
                    'statusId':statusId,
                    'stageId':'',
                    'mode':'',
                    'round':round,
                    'group':'',
                    'stageName':'',
                    'seasonYear':'',
                    'matchTime':matchTime,
                    'leagueName':leagueName,
                    'homeTeamId':homeTeamId,
                    'homeTeamName':homeTeamName,
                    'awayTeamId':awayTeamId,
                    'awayTeamName':awayTeamName,
                    'homeTeamRank':homeRank,
                    'awayTeamRank':awayRank,
                    'homeScore':homeScore,
                    'awayScore':awayScore,
                    'homeScoreHalf':homeHalfScore,
                    'awayScoreHalf':awayHalfScore,
                    'homeRedCard':homeRedCard,
                    'awayRedCard':awayRedCard,
                    'homeYellowCard':homeYellowCard,
                    'awayYellowCard':awayYellowCard,
                    'homeCorner':homeCorner,
                    'awayCorner':awayCorner,
                }
                matchdetail = self.increase(matchId, homeScore, awayScore)
                for name, value in matchdetail.items():
                    item_match[name] = value
                if level <= 2:
                    note = self.collection_4.find_one({'matchId':matchId})
                    if not note:
                        self.collection_4.insert_one(item_match)
                else:
                    note = self.collection_5.find_one({'matchId':matchId})
                    if not note:
                        self.collection_5.insert_one(item_match)
                logger.info('date:{}-matchId-->{}:赛程库里无此比赛,将直接存入赛果库'.format(date,matchId))
                continue
            logger.info('matchId为{}的比赛已比完，开始由赛程库转入赛果库......'.format(matchId))        
            if note_senior:
                if note_senior.get('delete','love') != 'love':
                    note_senior.pop('delete')
                note_senior['statusId'] = statusId
                note_senior['matchTime'] = matchTime
                note_senior['homeScore'] = homeScore
                note_senior['awayScore'] = awayScore
                note_senior['homeScoreHalf'] = homeHalfScore
                note_senior['awayScoreHalf'] = awayHalfScore
                note_senior['homeRedCard'] = homeRedCard
                note_senior['awayRedCard'] = awayRedCard
                note_senior['homeYellowCard'] = homeYellowCard
                note_senior['awayYellowCard'] = awayYellowCard
                note_senior['homeCorner'] = homeCorner
                note_senior['awayCorner'] = awayCorner
                note_senior['matchResult'] = item.get(result)
                note_senior['homeTeamRank'] = homeRank
                note_senior['awayTeamRank'] = awayRank
                if not note_senior.get('initHandicapResult') or not note_senior.get('initGoalLineResult'):
                    matchdetail = self.increase(matchId, homeScore, awayScore)
                    for name, value in matchdetail.items():
                        note_senior[name] = value
                else:
                    concede_points = note_senior.get('initHandicap').get('handicap')
                    goals_for = note_senior.get('initGoalLine').get('goalLine')
                    init_pan_road,init_large_ball = self.pankou(homeScore,awayScore,str(concede_points),str(goals_for))
                    note_senior['initHandicapResult'] = init_pan_road
                    note_senior['initGoalLineResult'] = init_large_ball
                    
                    pan_road, large_ball = self.pankou(homeScore,awayScore,str(finishHandicap_list[1]),str(finishGoalLine_list[1]))
                    note_senior['finishHandicap'] = {"homeOdds": finishHandicap_list[0],"handicap": finishHandicap_list[1],"awayOdds": finishHandicap_list[2]}
                    note_senior['finishHandicapResult'] = pan_road
                    note_senior['finishGoalLine'] = {"homeOdds": finishGoalLine_list[0],"goalLine": finishGoalLine_list[1],"awayOdds": finishGoalLine_list[2]}
                    note_senior['finishGoalLineResult'] = large_ball
                mids = self.collection_4.find_one({'matchId':matchId})
                if not mids:
                    self.collection_4.insert_one(note_senior)
                else:
                    self.collection_4.update_one({'matchId':matchId},{'$set':note_senior})
                    
                self.collection_2.update_one({'matchId':matchId},{'$set':{'delete':1}})
                logger.info('matchId为{}的比赛已比完，由赛程库转入赛果库成功'.format(matchId))
            else:
                if note:
                    if note.get('delete','love') != 'love':
                        note.pop('delete')
                    note['statusId'] = statusId
                    note['matchTime'] = matchTime
                    note['homeScore'] = homeScore
                    note['awayScore'] = awayScore
                    note['homeScoreHalf'] = homeHalfScore
                    note['awayScoreHalf'] = awayHalfScore
                    note['homeRedCard'] = homeRedCard
                    note['awayRedCard'] = awayRedCard
                    note['homeYellowCard'] = homeYellowCard
                    note['awayYellowCard'] = awayYellowCard
                    note['homeCorner'] = homeCorner
                    note['awayCorner'] = awayCorner
                    note['matchResult'] = item.get(result)
                    note['homeTeamRank'] = homeRank
                    note['awayTeamRank'] = awayRank
                    if not note.get('initHandicapResult') or not note.get('initGoalLineResult'):
                        matchdetail = self.increase(matchId, homeScore, awayScore)
                        for name, value in matchdetail.items():
                            note[name] = value
                    else:
                        concede_points = note.get('initHandicap').get('handicap')
                        goals_for = note.get('initGoalLine').get('goalLine')
                        init_pan_road,init_large_ball = self.pankou(homeScore,awayScore,str(concede_points),str(goals_for))
                        note['initHandicapResult'] = init_pan_road
                        note['initGoalLineResult'] = init_large_ball
                        
                        pan_road, large_ball = self.pankou(homeScore, awayScore, str(finishHandicap_list[1]),
                                                           str(finishGoalLine_list[1]))
                        note['finishHandicap'] = {"homeOdds": finishHandicap_list[0],
                                                  "handicap": finishHandicap_list[1],
                                                  "awayOdds": finishHandicap_list[2]}
                        note['finishHandicapResult'] = pan_road
                        note['finishGoalLine'] = {"homeOdds": finishGoalLine_list[0],
                                                  "goalLine": finishGoalLine_list[1],
                                                  "awayOdds": finishGoalLine_list[2]}
                        note['finishGoalLineResult'] = large_ball
                        
                    midg = self.collection_5.find_one({'matchId':matchId}) 
                    if not midg:
                        self.collection_5.insert_one(note)
                    else:
                        self.collection_5.update_one({'matchId':matchId},{'$set':note})
                        
                    self.collection_3.update_one({'matchId': matchId}, {'$set': {'delete': 1}})
                    logger.info('matchId为{}的比赛已比完，由赛程库转入赛果库成功'.format(matchId))
                else: #普通赛果集合
                    logger.info('赛程库没有此比赛：{}'.format(matchId))

    def pankou(self,homeScore,awayScore,concede_points,goals_for):
        
        if concede_points.isalpha():
            pan_road = ''
        else:
            if homeScore - awayScore > self.get_interger(concede_points):
                pan_road = 1  # 赢盘
            elif homeScore - awayScore < self.get_interger(concede_points):
                pan_road = -1  # 输盘
            else:
                pan_road = 0  # 走盘

        if goals_for.isalpha():
            large_ball = ''
        else:
            if homeScore + awayScore > self.get_interger(goals_for):
                large_ball = 1  # 大球
            elif homeScore + awayScore < self.get_interger(goals_for):
                large_ball = -1  # 小球
            else:
                large_ball = 0  #
        return pan_road,large_ball

    def select_database(self,matchId):
        note = self.collection_2.find_one({'matchId':matchId})
        note.pop('_id')

    def thread_pool2(self):
        li, liu = self.current_season()
        for r in liu:
            self.pool.apply_async(self.parse_data_saiguo, args=(r,))  # self.parse_data_saiguo(r)
        self.pool.close()
        self.pool.join()

        for i in li:
            self.pool.apply_async(self.parse_data_saiguo, args=(i,))  # self.parse_data_saiguo(i)
        self.pool.close()
        self.pool.join()

    def away(self):
        resp = self.get_data('https://live.leisu.com/saicheng?date=20190802')
        res = self.my_xpath(resp,'//*[@id="notStart"]/ul/li')
        print(len(res))

    def func(self,b):
        m = b + 2
        return m    
        
    def mac(self):
        print('小仙女')

if __name__ == '__main__':
    a = Match()  # 'https://data.leisu.com/team-40000'
    # a.thread_pool()
    # time.sleep(60)
    # a.thread_pool2()  # a.run_team2('https://data.leisu.com/team-10009')  # a.parse_data_saiguo(8782)
    while True:
        try:
            a.renewMatch()
            time.sleep(60*60*5)
        except Exception as e:
            logger.info('发生错误:{}，将继续重新匹配！'.format(e))
