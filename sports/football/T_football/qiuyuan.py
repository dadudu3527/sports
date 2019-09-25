import random
import logging
import re
import json
import time
from multiprocessing.pool import ThreadPool

import requests
from lxml import etree
import pymongo
import redis

from user_agent import agents
from config import config

logger = logging.getLogger(__name__)
fh = logging.FileHandler('C:\\sports\\football\\data\\qiuyuan_p.log')
sh = logging.StreamHandler()
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


class PlayerD:
    def __init__(self):
        self.a = 0
        client_a = pymongo.MongoClient(config)
        # client = pymongo.MongoClient()
        self.pool = ThreadPool(6)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
        dbname = client_a['football']
        # dbname = client['demo2']
        self.col_read_team = dbname['team']
        self.col_read_player = dbname['player']
        self.collection_TransferMarket = dbname['playerTransferMarket']  # 球员转会信息
        self.collection_MarketValue = dbname['playerMarketValue']  # 球员身价浮动
        self.collection_Characteristic = dbname['playerCharacteristic']  # 球员技术特点
        self.collection_Detail = dbname['playerDetail']  # 球员详情

    def my_xpath(self, data, ruler):
        return etree.HTML(data).xpath(ruler)

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
                raise Exception('递归次数超过20!')

    def get_data(self, url):
        resp = self.getData(url, self.a)
        return resp.text


    def get_team_id(self, teamName):  # resp是一个元组
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

    def get_player_id(self, playerName_1, playerName_en, teamId,*args):  # resp是一个元组
        contractEndDay, countryZh, birthday = (args)
        playerName = playerName_1.split('·')
        if teamId:
            note = self.col_read_player.find_one({'nameZH': playerName_1, 'teamId': teamId, },{'playerId': 1})
            if not note:
                note = self.col_read_player.find_one({'nameZH': {'$regex': playerName[-1]}, 'teamId': teamId,},{'playerId': 1})
                if not note:
                    note = self.col_read_player.find_one({'nameZH': {'$regex': playerName[0]}, 'teamId': teamId, },{'playerId': 1})
                    if not note:
                        if playerName_en:
                            playerName_en = playerName_en.strip()
                            Name_en = playerName_en.split(' ')
                            note = self.col_read_player.find_one({'nameEN': playerName_en, 'teamId': teamId},{'playerId': 1})
                            if not note:
                                note = self.col_read_player.find_one({'nameEN': playerName_en.strip().replace(' ', '-'), 'teamId': teamId},{'playerId': 1})
                                if not note:
                                    note = self.col_read_player.find_one({'nameEN': playerName_en.strip().replace(' ', '·'), 'teamId': teamId},{'playerId': 1})
                                    if not note:
                                        note = self.col_read_player.find_one({'teamId': teamId, 'nameEN': {'$regex': Name_en[-1]}}, {'playerId': 1})
                                        if not note:
                                            note = self.col_read_player.find_one({'teamId': teamId, 'nameEN': {'$regex': Name_en[0]}}, {'playerId': 1})
                                            if not note:
                                                note = self.col_read_player.find_one({'teamId': teamId, 'birthday':birthday,'country':countryZh,'contractEndDay':contractEndDay},{'playerId': 1})
                                                if not note:
                                                    note = self.col_read_player.find_one({'teamId': teamId, 'birthday': birthday, 'country': countryZh}, {'playerId': 1})
            if note:
                playerId = note.get('playerId')
                print('{}:添加playerId成功！'.format(playerId))
            else:
                raise Exception('playerId')
        else:
            note = self.col_read_player.find_one({'nameZH': playerName_1,'playerName_en':playerName_en, 'birthday': birthday, 'country':countryZh}, {'playerId': 1})
            if not note:
                self.col_read_player.find_one({'nameZH': playerName_1, 'birthday': birthday, 'country': countryZh},{'playerId': 1})
                if not note:
                    note = self.col_read_player.find_one({'nameEN': playerName_en, 'birthday': birthday,'country': countryZh}, {'playerId': 1})

            if note:
                playerId = note.get('playerId')
                print('{}:添加playerId成功！'.format(playerId))
            else:
                raise Exception('playerId')

        return playerId

    def parse_transfer(self, playerid, playerId, playerName, arg):  # 球员转会 transferMarket
        playerName_en, teamId, teamName, countryZh, playerMP = arg
        url = 'http://www.tzuqiu.cc/players/{}/transferList.do'.format(playerid)
        resp = self.get_data(url)
        # playerName_zh = self.is_empty(self.my_xpath(resp,'//div[@class="row head_info"]/div[1]/h1/text()'))
        # playerName_en = self.is_empty(self.my_xpath(resp,'//div[@class="player-info"]/div/div[1]/table//tr[1]/td/text()'))
        tr = self.my_xpath(resp, '//div[@class="row"]/div[1]/table/tbody/tr')
        for i in range(1, len(tr) + 1):
            season = self.is_empty(
                self.my_xpath(resp, '//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[1]/text()'.format(i)))
            date = self.is_empty(
                self.my_xpath(resp, '//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[2]/text()'.format(i)))
            leftTeamName = self.is_empty(
                self.my_xpath(resp, 'string(//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[3])'.format(i))).replace(
                '\r', '').replace('\n', '').replace('\t', '')
            leftTeamId = self.get_team_id(leftTeamName)
            joinedTeamName = self.is_empty(
                self.my_xpath(resp, 'string(//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[4])'.format(i))).replace(
                '\r', '').replace('\n', '').replace('\t', '')
            joinedTeamId = self.get_team_id(joinedTeamName)
            marketValue = self.is_empty(
                self.my_xpath(resp, '//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[5]/text()'.format(i)))
            transferFee = self.is_empty(
                self.my_xpath(resp, '//div[@class="row"]/div[1]/table/tbody/tr[{}]/td[7]/text()'.format(i)))
            item = {'playerId': playerId, 'playerName': playerName, 'playerName_en': playerName_en, 'teamId': teamId,
                'teamName': teamName, 'countryZh': countryZh, 'playerMainPosition': playerMP, 'season': season,
                'date': date, 'leftTeamId': leftTeamId, 'leftTeamName': leftTeamName, 'joinedTeamId': joinedTeamId,
                'joinedTeamName': joinedTeamName, 'marketValue': marketValue, 'transferFee': transferFee}
            # print(item)
            self.save_database(self.collection_TransferMarket, playerid, playerId, item, season=season)

    def tech_character(self, playerid, playerId, playerName_zh, arg):  # 球员技术特点
        playerName_en, teamId, teamName, countryZh, playerMP,birthday,level = arg
        url = 'http://www.tzuqiu.cc/players/{}/show.do'.format(playerid)
        resp = self.get_data(url)
        # playerName_zh = self.is_empty(self.my_xpath(resp, '//div[@class="row head_info"]/div[1]/h1/text()'))
        # playerName_en = self.is_empty(self.my_xpath(resp,'//div[@class="player-info"]/div/div[1]/table//tr[1]/td/text()'))
        # playerId = self.get_player_id((resp,playerName_zh,playerName_en))
        it = {'event-icon icon-ptn offensive': 1, 'event-icon icon-ptn defensive': 2}  # 1代表攻 2代表防
        strengths = []
        weaknesses = []
        playStype = []
        s_tr = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[1]/table//tr')
        for i in range(1, len(s_tr) + 1):
            strengthName = self.my_xpath(resp,
                                         '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[1]/div/text()'.format(
                                             i))
            strengthName = ''.join(strengthName).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            gongfang_s = self.my_xpath(resp,
                                       '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[1]/div/span/@class'.format(
                                           i))[0]
            strengthLevel = self.is_empty(self.my_xpath(resp,
                                                        '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[2]/span/text()'.format(
                                                            i)))
            strength = [it.get(gongfang_s), strengthName, strengthLevel]
            strengths.append(strength)
        w_tr = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[2]/table//tr')
        for i in range(1, len(w_tr) + 1):
            weaknessName = self.my_xpath(resp,
                                         '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[1]/div/text()'.format(
                                             i))
            weaknessName = ''.join(weaknessName).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            gongfang_w = self.my_xpath(resp,
                                       '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[1]/div/span/@class'.format(
                                           i))[0]
            weaknessLevel = self.is_empty(self.my_xpath(resp,
                                                        '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[2]/span/text()'.format(
                                                            i)))
            weakness = [it.get(gongfang_w), weaknessName, weaknessLevel]
            weaknesses.append(weakness)
        p_tr = self.my_xpath(resp, '//div[@class="character"]/div[2]/div[1]/table//tr')
        for i in range(1, len(p_tr) + 1):
            gongfang_p = self.my_xpath(resp,
                                       '//div[@class="character"]/div[2]/div[1]/table//tr[{}]/td[1]/div/span/@class'.format(
                                           i))[0]
            playS = self.my_xpath(resp,
                                  '//div[@class="character"]/div[2]/div[1]/table//tr[{}]/td[1]/div/text()'.format(i))
            playS = ''.join(playS).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            play_li = [it.get(gongfang_p), playS]
            playStype.append(play_li)
        item = {'playerId': playerId, 'playerName': playerName_zh, 'playerName_en': playerName_en, 'teamId': teamId,
            'teamName': teamName, 'countryZh': countryZh, 'playerMainPosition': playerMP,'birthday':birthday,'level':level, 'strengths': strengths,
            'weaknesses': weaknesses, 'playStype': playStype, }
        # print(item)
        return item

    def playerMarketValue(self, data, playerId, playerName, arg):  # 球员身价浮动
        playerName_en, teamId, teamName, countryZh, playerMP = arg
        marketValueDetail = []
        item = data
        highestDate = item.get('highestMvDate')
        highestWorth = item.get('highestMvValue')
        currentDate = item.get('currentMvDate')
        currentWorth = item.get('currentMvValue')
        date_list = item.get('categories')
        marker_list = item.get('serial')
        for date, marker in zip(date_list, marker_list):
            value = marker.get('y')
            teamName = marker.get('verein')
            age = marker.get('age')
            item = {'date': date, 'value': value, 'teamName': teamName, 'age': age}
            marketValueDetail.append(item)
        playerMarketValue = {'playerId': playerId, 'playerName': playerName, 'playerName_en': playerName_en,
            'teamId': teamId, 'teamName': teamName, 'countryZh': countryZh, 'playerMainPosition': playerMP,
            'highestDate': highestDate, 'highestWorth': highestWorth, 'currentDate': currentDate,
            'currentWorth': currentWorth, 'marketValueDetail': marketValueDetail, }
        # print(playerMarketValue)
        return playerMarketValue

    def player_detail(self, playerid, playerId, season, playerName, arg):  # 球员详细数据
        playerName_en, teamId, teamName, countryZh, playerMP = arg
        season_1 = season.replace('/', '-')
        url = 'http://www.tzuqiu.cc/players/{}/season/{}/seasonStats.do'.format(playerid, season_1)
        resp = self.get_data(url)
        itemName = {'红黄牌': 'YRC', '出场': 'APPS', '传球成功率': 'PSP', '场均关键传球': 'KPPG', '创造绝佳机会': 'CEC', '场均争顶成功': 'HSPG',
            '评分': 'SCORE', '进球': 'GOALS', '场均射门': 'SPG', '场均射正': 'SOTPG', '把握机会能力': 'CTSO', '场均过人': 'CPG',
            '场均被侵犯': 'FPG', '场均越位': 'OPG', '场均抢断': 'TPG', '场均拦截': 'IPG', '场均解围': 'CPG', '场均封堵': 'BPG', '场均造越位': 'MOPG',
            '场均犯规': 'FGP', '致命失误': 'FM', '助攻': 'Assist', '场均传球': 'PPG', '前场传球成功率': 'RPSP', '场均夺回球权': 'RBPG',
            '射正率': 'SOTP', '进球率': 'GP', '把握机会能力1': 'CTSOD', '传球成功率1': 'PSPD', '前场传球成功率1': 'RPSPD', '中场传球成功率': 'MPSP',
            '后场传球成功率': 'BPSP', '传中成功率': 'CBSP', '长传成功率': 'LBSP', '直塞成功率': 'TBSP', '致命失误/失误': 'FMD'}
        item = {'trend trend-hugerise': 2, 'trend trend-rise': 1, 'trend trend-flat': 0, 'trend trend-fall': -1,
                'trend trend-hugefall': -2, '': ''}
        player_detail_item = {'playerId': playerId, 'season': season, 'playerName': playerName,
            'playerName_en': playerName_en, 'teamId': teamId, 'teamName': teamName, 'countryZh': countryZh,
            'playerMainPosition': playerMP, }
        seasonDiv = self.my_xpath(resp, '//div[@class="smallTab"]/div')
        data_list = []
        for i in range(2, len(seasonDiv) + 1):
            data_id = self.is_empty(self.my_xpath(resp, '//div[@class="smallTab"]/div[{}]/@dataid'.format(i)))
            leagueName = self.is_empty(self.my_xpath(resp, '//a[@dataid={}]/@competition'.format(data_id)))
            row_statPre = self.my_xpath(resp, '//div[@class="smallTab"]/div[{}]/div'.format(i))
            data_item = {'league': leagueName}
            for r in range(1, len(row_statPre) + 1):
                if r == 1 or r == 2:  # 前俩栏数据
                    text_div = self.my_xpath(resp, '//div[@class="smallTab"]/div[{}]/div[{}]/div'.format(i, r))
                    for d in range(1, len(text_div) + 1):
                        table_tr = self.my_xpath(resp,
                                                 '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr'.format(
                                                     i, r, d))
                        for x in range(1, len(table_tr) + 1):
                            # table_td = self.my_xpath(resp, '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td'.format(i, r, d, x))
                            if r == 1 and d == 1 and x == 1:  # 红黄牌
                                name = self.my_xpath(resp,
                                                     '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[1]/text()'.format(
                                                         i, r, d, x))[0]
                                ability = self.my_xpath(resp,
                                                        '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span/text()'.format(
                                                            i, r, d, x))
                                teamRank = self.my_xpath(resp,
                                                         '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[3]/span/text()'.format(
                                                             i, r, d, x))
                                teamRank = ''.join(teamRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                    ' ', '')
                                positionRank = self.my_xpath(resp,
                                                             '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[4]/span/text()'.format(
                                                                 i, r, d, x))
                                positionRank = ''.join(positionRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                    ' ', '')
                                ability.append(teamRank)
                                ability.append(positionRank)
                                li = self.transfer_int(ability)
                                data_item[itemName.get(name, name)] = li
                            elif r == 1 and d == 1 and x == 2:  # 出场次数
                                name = self.my_xpath(resp,
                                                     '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[1]/text()'.format(
                                                         i, r, d, x))[0]
                                ability = self.my_xpath(resp,
                                                        '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span/text()'.format(
                                                            i, r, d, x))
                                ability = ''.join(ability).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                       '').replace(' ',
                                                                                                                   '')
                                ability_1 = self.get_interger(ability.split('(')[0])
                                ability_2 = self.get_interger(ability.split('(')[-1].split(')')[0])
                                teamRank = self.my_xpath(resp,
                                                         '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[3]/span/text()'.format(
                                                             i, r, d, x))
                                teamRank = ''.join(teamRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                    ' ', '')
                                positionRank = self.my_xpath(resp,
                                                             '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[4]/span/text()'.format(
                                                                 i, r, d, x))
                                positionRank = ''.join(positionRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                    ' ', '')
                                li = [self.get_interger(ability_1), self.get_interger(ability_2),
                                      self.get_interger(teamRank), self.get_interger(positionRank)]
                                data_item[itemName.get(name, name)] = li

                            else:  # 其他
                                name = self.my_xpath(resp,
                                                     '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[1]/text()'.format(
                                                         i, r, d, x))[0]
                                ability = self.my_xpath(resp,
                                                        '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span[1]/text()'.format(
                                                            i, r, d, x))
                                ability = ''.join(ability).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                       '').replace(' ',
                                                                                                                   '')
                                trend = self.is_empty(self.my_xpath(resp,
                                                                    '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[2]/span[2]/@class'.format(
                                                                        i, r, d, x)))
                                teamRank = self.my_xpath(resp,
                                                         '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[3]/span/text()'.format(
                                                             i, r, d, x))
                                teamRank = ''.join(teamRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                         '').replace(
                                    ' ', '')
                                positionRank = self.my_xpath(resp,
                                                             '//div[@class="smallTab"]/div[{}]/div[{}]/div[{}]/table/tbody/tr[{}]/td[4]/span/text()'.format(
                                                                 i, r, d, x))
                                positionRank = ''.join(positionRank).replace('\r', '').replace('\n', '').replace('\t',
                                                                                                                 '').replace(
                                    ' ', '')
                                li = [self.get_interger(ability), item.get(trend), self.get_interger(teamRank),
                                      self.get_interger(positionRank)]
                                data_item[itemName.get(name, name)] = li
                else:  # 后三栏的数据
                    text_tr = self.my_xpath(resp,
                                            '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr'.format(i,
                                                                                                                    r))
                    for tr in range(1, len(text_tr) + 1):
                        name = self.my_xpath(resp,
                                             '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[1]/text()'.format(
                                                 i, r, tr))[0]
                        if name == '把握机会能力':
                            name = '把握机会能力1'
                        elif name == '传球成功率':
                            name = '传球成功率1'
                        elif name == '前场传球成功率':
                            name = '前场传球成功率1'
                        value_1 = self.my_xpath(resp,
                                                '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[2]/span/span[1]/span/text()'.format(
                                                    i, r, tr))
                        value_1 = ''.join(value_1).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ',
                                                                                                                 '')
                        value_2 = self.my_xpath(resp,
                                                '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[2]/span/span[2]/text()'.format(
                                                    i, r, tr))
                        value_2 = ''.join(value_2).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ',
                                                                                                                 '')
                        trend = self.is_empty(self.my_xpath(resp,
                                                            '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[4]/span/@class'.format(
                                                                i, r, tr)))
                        teamRank = self.my_xpath(resp,
                                                 '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[5]/span/text()'.format(
                                                     i, r, tr))
                        teamRank = ''.join(teamRank).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ',
                                                                                                                   '')
                        positionRank = self.my_xpath(resp,
                                                     '//div[@class="smallTab"]/div[{}]/div[{}]/div[1]/table/tbody/tr[{}]/td[6]/span/text()'.format(
                                                         i, r, tr))
                        positionRank = ''.join(positionRank).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                        li = [self.get_interger(value_1), self.get_interger(value_2), item.get(trend),
                              self.get_interger(teamRank), self.get_interger(positionRank)]
                        data_item[itemName.get(name, name)] = li
            data_list.append(data_item)
        player_detail_item['data'] = data_list
        # print(player_detail_item)
        return player_detail_item

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

    def re_parse(self, resp):
        try:
            data = json.loads(re.findall(r'var mvData *= *(\{.*?\}\]\});', resp)[0])
        except:
            data = {}
        # print(data)
        return data

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

    def saveFile(self, teamidT):
        with open('C:\\sports\\football\\data\\playerid_Tz.json', 'a') as f:
            f.write(json.dumps(teamidT, ensure_ascii=False) + '/,')

    def save_database(self, collect, id, playerId, playerData, season=''):
        if playerId:
            if not season:
                note = collect.find_one({'playerId': playerId})
                if not note:
                    collect.insert_one(playerData)
                else:
                    playerid = note.get('playerId')
                    if playerid:  # and not teamData['teamId']
                        playerData['playerId'] = playerid
                    collect.update_one({'playerId': playerId}, {'$set': playerData})
            else:
                note = collect.find_one({'playerId': playerId, 'season': season})
                if not note:
                    collect.insert_one(playerData)
                else:
                    playerid = note.get('playerId')
                    if playerid:  # and not teamData['teamId']
                        playerData['playerId'] = playerid
                        collect.update_one({'playerId': playerId, 'season': season}, {'$set': playerData})
        else:
            if not season:
                note = collect.find_one({'playerName': playerData.get('playerName')})
                if not note:
                    collect.insert_one(playerData)
                else:
                    playerid = note.get('playerId')
                    if playerid:  # and not teamData['teamId']
                        playerData['playerId'] = playerid
                    collect.update_one({'playerName': playerData.get('playerName')}, {'$set': playerData})
            else:
                note = collect.find_one({'playerName': playerData.get('playerName'), 'season': season})
                if not note:
                    collect.insert_one(playerData)
                else:
                    playerid = note.get('playerId')
                    if playerid:  # and not teamData['teamId']
                        playerData['playerId'] = playerid
                        collect.update_one({'playerName': playerData.get('playerName'), 'season': season},
                                           {'$set': playerData})  # logger.info('{}数据下载成功！'.format(id))

    def all_player_detail(self, playerid, playerId, name, arg):
        url = 'http://www.tzuqiu.cc/players/{}/seasonStats.do'.format(playerid)
        resp = self.get_data(url)
        season = self.my_xpath(resp, '//form[@class="form-inline"]/div[1]/div[1]/ul/li/a/text()')
        for i in season:
            item = self.player_detail(playerid, playerId, i, name, arg)
            self.save_database(self.collection_Detail, playerid, playerId, item, season=i)

    def spider_player(self, name, id, teamName):
        print('{}：数据开始下载'.format(id))
        try:
            playerid = int(id)
            url = 'http://www.tzuqiu.cc/players/{}/show.do'.format(id)
            resp = self.get_data(url)
            teamId = self.get_team_id(teamName)
            playerName_en = self.is_empty(self.my_xpath(resp, '//div[@class="player-info"]/div/div[1]/table//tr[1]/td/text()'))
            playerMP = self.is_empty(
                self.my_xpath(resp, '//div[@class="player-info"]/div/div[1]/table//tr[3]/td/text()'))
            if playerMP:
                playerMP = playerMP.split('&nbsp')[0]
            contractEndDay = self.is_empty(
                self.my_xpath(resp, '//div[@class="player-info"]/div/div[1]/table//tr[5]/td/text()'))
            birthday = self.is_empty(
                self.my_xpath(resp, '//div[@class="player-info"]/div/div[2]/table//tr[1]/td/text()'))
            countryZh = self.is_empty(
                self.my_xpath(resp, '//div[@class="player-info"]/div/div[2]/table//tr[4]/td/a/text()'))
            if birthday:
                birthday = re.findall(r'.*?\((.*?)\)', birthday)[0]
            level = self.is_empty(self.my_xpath(resp, '//*[@id="summaryTable"]/tbody/tr[1]/td[last()]/span/text()'))    
             
            playerId = self.get_player_id(name,playerName_en,teamId,contractEndDay,countryZh,birthday)
            args = (playerName_en, teamId, teamName, countryZh, playerMP,birthday,level)
            character = self.tech_character(playerid, playerId, name, args)  # 技术特点
            self.save_database(self.collection_Characteristic, playerid, playerId, character)

            marketValue = self.playerMarketValue(self.re_parse(resp), playerId, name, args)  # 身价走势 #匹配身价信息
            self.save_database(self.collection_MarketValue, playerid, playerId, marketValue)

            self.parse_transfer(playerid, playerId, name, args)  # 转会信息
            self.all_player_detail(playerid, playerId, name, args)  # 球员详情
            print('{}：数据下载成功！'.format(id))

        except Exception as e:
            w = '{}-{}-{}'.format(name,id,teamName)
            self.mdb.sadd('qiuyuan',a)
            # self.saveFile({'playerName': name, 'playerid': id, 'teamName': teamName})
            print('playerid:{},数据入库失败-->{}'.format(id, e))

    def run(self):
        for name, id, teamName, countryZh, playerMP in self.get_playerUrl():
            self.pool.apply_async(self.spider_player, args=(name, id, teamName))  # self.spider_player(name,id,teamName)
        self.pool.close()
        self.pool.join()

    def take_team_info(self):  #获取球队名称和Id
        li = []
        for i in range(1, 14):
            url = 'http://www.tzuqiu.cc/competitions/{}/show.do'.format(i)
            resp = self.get_data(url=url)
            team_url = self.my_xpath(resp,'//*[@id="rankTable0"]/tbody/tr/td[2]/a/@href')
            if i == 1:
                team_name = self.my_xpath(resp,'//*[@id="rankTable0"]/tbody/tr/td[2]/a/text()')
                index_mc = team_name.index('曼城')
                index_ml= team_name.index('曼联')
                team_name.remove('曼城')
                team_name.insert(index_mc,'曼彻斯特城')
                team_name.remove('曼联')
                team_name.insert(index_ml, '曼彻斯特联')
            else:
                team_name = self.my_xpath(resp, '//*[@id="rankTable0"]/tbody/tr/td[2]/a/text()')
            for r in zip(team_name,team_url):
                li.append(r)
        # print(li)
        return  li

    def take_player_info(self,li): #获取球队,球员名称和Id
        data_li = []
        for teamName,teamUrl in li:
            url = 'http://www.tzuqiu.cc' + teamUrl.strip()
            try:
                resp = self.get_data(url)
                player_url = self.my_xpath(resp,'//*[@id="playersTable"]/tbody/tr/td[2]/a/@href')
                player_name = self.my_xpath(resp,'//*[@id="playersTable"]/tbody/tr/td[2]/a/@title')
                if len(player_url) == len(player_name):
                    for playerName,playerUrl in zip(player_name,player_url):
                        playerId = re.findall(r'/players/(\d+)/show\.do',playerUrl)[0]
                        # resp = self.get_data(playerUrl)
                        data = (playerId.strip(),playerName.strip(),teamName.strip())
                        data_li.append(data)
                        # print(data)
                else:
                    raise Exception('字段长度不匹配！')
            except Exception as e:
                print('teamUrl{}:页面访问失败,{}'.format(teamUrl,e))
                logger.info('{}-{}'.format(teamName,teamUrl))
        print('已获取球员Id,接下来开始抓取！')
        return data_li

    def run_player(self):
        li = self.take_team_info()
        for  id,name,teamName in self.take_player_info(li):
            self.pool.apply_async(self.spider_player, args=(name, id, teamName))  # self.spider_player(name,id,teamName)
        self.pool.close()
        self.pool.join()

    def get_playerUrl(self):
        li = []
        li_name = []
        li_tname = []
        li_pos = []
        li_cou = []
        url = 'http://www.tzuqiu.cc/playerStatistics/querys.json?start=0&length=1&extra_param%5BisCurrentSeason%5D=true&extra_param%5BcompetitionRange%5D=all&extra_param%5BorderCdnReq%5D=true&extra_param%5BsatisfyAttendance%5D=true'
        resp = self.getData(url=url, a=self.a).text
        resp = json.loads(resp)
        count = resp.get('recordsTotal')
        url_con = 'http://www.tzuqiu.cc/playerStatistics/querys.json?start=0&length={}&extra_param%5BisCurrentSeason%5D=true&extra_param%5BcompetitionRange%5D=all&extra_param%5BorderCdnReq%5D=true&extra_param%5BsatisfyAttendance%5D=true'.format(
            count)
        res = self.getData(url=url_con, a=self.a).text
        res = json.loads(res)
        for i in res.get('data'):
            playerId = i.get('playerId')
            playerName = i.get('playerName')
            teamName = i.get('teamName')
            country = i.get('countryZh')
            playerMP = i.get('playerMainPosition')

            li.append(playerId)
            li_name.append(playerName)
            li_tname.append(teamName)
            li_cou.append(country)
            li_pos.append(playerMP)
        print(len(li), len(li_name), len(li_tname))
        return zip(li_name, li, li_tname, li_cou, li_pos)


if __name__ == '__main__':
    a = PlayerD()
    # a.run_player()
    while True:
        w = a.mdb.spop('qiuyuan')
        if w:
            args = tuple(w.split('-'))
            a.spider_player(*args)
        else:
            break
    # while True:
        # a.run()
        # x = 0
        # with open('C:\\sports\\football\\data\\playerid_Tz.json', 'r') as f:
            # print('开始读取文件。。。')
            # id_str = f.read()
            # if id_str:
                # id_list = set(id_str.split('/,'))
                # for i in id_list:
                    # if i != '':
                        # d = json.loads(i)
                        # a.spider_player(d.get('playerName'), d.get('playerid'), d.get('teamName'))
        # time.sleep(60 * 60 * 24 * 3)
        # with open('C:\\sports\\football\\data\\playerid_Tz.json', 'w') as f:
            # f.write('')  # a.all_player_detail(11,11)