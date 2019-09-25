import requests
from lxml import etree
from user_agent import agents
from multiprocessing.pool import ThreadPool
import pymongo
import random
import logging
import re
import json
import time

from user_agent import agents
from config import config

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class TeamD:
    def __init__(self):
        self.a = 0
        self.pool = ThreadPool(5)
        client = pymongo.MongoClient(config)
        dbname = client['football']
        self.col_read_team = dbname['team']
        self.col_read_player = dbname['player']

        self.collection_m = dbname['TeamMatchDetailData']
        self.collection_a = dbname['TeamAdditionalInfo']

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

    def get_data(self,url='http://www.tzuqiu.cc/players/11/transferList.do'):

        # headers = {
        #     'User-Agent': random.choice(agents)
        # }
        # resp = requests.get(url=url,headers=headers)
        # if resp.status_code == 200:
        #     pass
        # else:
        resp = self.getData(url,self.a)

        return resp.text

    def get_team_id(self,teamName): #resp是一个元组
        note = self.col_read_team.find_one({'name_zh':teamName},{'teamId':1})
        if note:
            teamId = note.get('teamId')
        else:
            note = self.col_read_team.find_one({'name_zh':{'$regex':teamName}},{'teamId': 1})
            if note:
                teamId = note.get('teamId')
            else:
                teamId = ''
        return teamId

    def get_player_id(self,playerName_1,teamName,leagueAbbr): 
        name = '{}({})'.format(teamName,leagueAbbr)
        note = self.col_read_player.find_one({'nameZH':{'$regex':playerName_1},'teamBelongs':name},{'playerId':1,'teamBelongs':1})
        if not note:
            note = self.col_read_player.find_one({'nameZHAbbr': playerName_1,'teamBelongs':name}, {'playerId': 1, 'teamBelongs': 1})
        if note:
            playerId = note.get('playerId')
        # elif len(list(note)) > 1:
            # note = self.col_read_player.find({'nameZHAbbr': {'$regex': playerName_1}, 'teamBelongs': name},{'playerId': 1, 'teamBelongs': 1})
            # if len(list(note)) == 1:
                # playerId = list(note)[0].get('playerId')
            # else:
                # playerId = ''
        else:
            # raise Exception('球队未找到')
            playerId = ''
        return playerId

    def team_detail(self,teamid,teamId,name,season,leagueAbbr): #球队详细数据
        url = 'http://www.tzuqiu.cc/teams/{}/teamStats.do'.format(teamid) #http://www.tzuqiu.cc/teams/78/teamStats.do
        resp = self.get_data(url)
        itemName = {
            'Cards': 'YRC',  # 红黄牌
            'possession':'PP',#控球率
            'passSucc':'PSP',#传球成功率
            'keyPasses':'KPPG',#场均关键传球
            'bigChanceCreated':'CEC',#创造绝佳机会
            'aerialWon':'HSPG',#场均争顶成功
            'rate':'TARING',#评分
            'goals':'GOALS',#进球
            'shots':'SPG',#场均射门
            'shotsOT':'SOTPG',#场均射正
            'bigChanceSucc':'CTSO',#把握机会能力
            'dribbles':'CGR',#场均过人
            'fouled':'FPG',#场均被侵犯
            'offsides':'OPG',#场均越位
            'goalsLost':'GA',#失球
            'shotsConceded':'CGPG',#场均被射门
            'tackles':'TPG',#场均抢断
            'interceptions':'IPG',#场均拦截
            'clears':'CPG',#场均解围
            'fouls':'FGP',#场均犯规
            'errorsSum':'FM',#致命失误
            'assists':'Assist',#助攻
            'finalThirdPassSucc':'RPSP',#前场传球成功率
            'ballRecovery':'RBPG',#场均夺回球权
        }
        player_detail_item = {'teamId':teamId,'teamName':name,'leagueAbbr':leagueAbbr,'season':season}
        data = []
        leagueName = self.my_xpath(resp,'//dl[@id="competition-list"]/dd/a/text()')
        competitionid = self.my_xpath(resp,'//dl[@id="competition-list"]/dd/a/@competitionid')
        # seasonYear = self.my_xpath(resp,'//dl[@id="competition-list"]/dd/a/@season')
        for league,com in zip(leagueName,competitionid):
            dic = {'league':league}
            url = 'http://www.tzuqiu.cc/teamStatistics/querysTeamStatPtn.json?competitionId={}&season={}&teamId={}'.format(com, season, teamid)
            res = self.get_data(url)
            try:
                item = json.loads(res)
            except:
                raise Exception('215行的json.loads(),转化失败！')
            for key,value in itemName.items():
                if key == 'Cards':
                    redCards = item.get('redCards')
                    yelCards = item.get('yelCards')
                    cardPtn = item.get('cardPtn')
                    li = [yelCards,redCards,cardPtn]
                    dic[value] = li
                else:
                    ability = item.get(key)
                    rank = item.get('{}Ptn'.format(key))
                    li = [round(ability,3),rank]
                    dic[value] = li
            data.append(dic)
        player_detail_item['data'] = data

        playTable = {}
        type_li = ['goalType','shotType','passType']
        for d in type_li:
            tr = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr'.format(d)) #shotTypeTable
            list_all = []
            for a in range(1,len(tr)+1):
                list_1 = []
                td = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td'.format(d,a))
                for i in range(1,len(td)+1):
                    if i == 2:
                        go = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[2]/span/span/span/text()'.format(d,a))
                        go = ''.join(go).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                        go = self.move_bf(go)
                        list_1.append(go)
                    else:
                        zb = self.my_xpath(resp,'//div[@id="{}"]/div[1]/table/tbody/tr[{}]/td[{}]/text()'.format(d,a,i))
                        zb = ''.join(zb).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                        zb = self.move_bf(zb)
                        list_1.append(zb)
                list_all.append(list_1)
            playTable[d] = list_all
        player_detail_item['playType'] = playTable

        jingong = ['attackSide','shotDirection','shotZone']
        attack_item = {}
        for j in jingong:
            attack_li = []
            div = self.my_xpath(resp,'//div[@id="{}"]/div[1]/div[1]/div[1]/div'.format(j))
            for n in range(1,len(div)+1):
                att_li = []
                d_li = self.my_xpath(resp,'//div[@id="{}"]/div[1]/div[1]/div[1]/div[{}]/div[1]/span/text()'.format(j,n))
                d_li.reverse()
                for l in d_li:
                    l = l.replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
                    l = self.move_bf(l)
                    att_li.append(l)
                attack_li.append(att_li)
            attack_item[j] = attack_li
        player_detail_item['attackDistribution'] = attack_item
        # print(player_detail_item)
        return player_detail_item

    def characteristics(self,resp,teamId,name,season,leagueAbbr): #球队技术特点
        # teamName_zh = self.is_empty(self.my_xpath(resp, '//div[@class="row head_info"]/div[1]/h1/text()'))
        it = {'event-icon icon-ptn offensive':1,'event-icon icon-ptn defensive':2} #1代表攻 2代表防
        strengths = []
        weaknesses = []
        playStype = []
        s_tr = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[1]/table//tr')
        for i in range(1,len(s_tr)+1):
            strengthName = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[1]/div/text()'.format(i))
            strengthName = ''.join(strengthName).replace('\r','').replace('\n','').replace('\t','').replace(' ','')
            gongfang_s = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[1]/div/span/@class'.format(i))[0]
            strengthLevel = self.is_empty(self.my_xpath(resp, '//div[@class="character"]/div[1]/div[1]/table//tr[{}]/td[2]/span/text()'.format(i)))
            strength = [it.get(gongfang_s),strengthName,strengthLevel]
            strengths.append(strength)
        w_tr = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[2]/table//tr')
        for i in range(1,len(w_tr)+1):
            weaknessName = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[1]/div/text()'.format(i))
            weaknessName = ''.join(weaknessName).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            gongfang_w = self.my_xpath(resp, '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[1]/div/span/@class'.format(i))[0]
            weaknessLevel = self.is_empty(self.my_xpath(resp, '//div[@class="character"]/div[1]/div[2]/table//tr[{}]/td[2]/span/text()'.format(i)))
            weakness = [it.get(gongfang_w),weaknessName,weaknessLevel]
            weaknesses.append(weakness)
        p_tr = self.my_xpath(resp, '//div[@class="character"]/div[2]/div[1]/table//tr')
        for i in range(1,len(p_tr)+1):
            gongfang_p = self.my_xpath(resp, '//div[@class="character"]/div[2]/div[1]/table//tr[{}]/td[1]/div/span/@class'.format(i))[0]
            playS = self.my_xpath(resp, '//div[@class="character"]/div[2]/div[1]/table//tr[{}]/td[1]/div/text()'.format(i))
            playS = ''.join(playS).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            play_li = [it.get(gongfang_p),playS]
            playStype.append(play_li)
        item = {
            'teamId':teamId,
            'teamName':name,
            'leagueAbbr':leagueAbbr,
            'season':season,
            'characteristics':{
                'strengths':strengths,
                'weaknesses':weaknesses,
                'playStype':playStype,
            }
        }
        # print(item)
        return item

    def rankDetails(self,resp):
        rank_list = []
        it = {'event-icon icon-ptn offensive': 1, 'event-icon icon-ptn defensive': 2}  # 1代表攻 2代表防
        tr = self.my_xpath(resp,'//table[@class="style team-stat-dsp"]/tbody/tr')
        for i in range(1,len(tr)+1):
            type = self.my_xpath(resp, '//table[@class="style team-stat-dsp"]/tbody/tr[{}]/td[1]/div/span[1]/@class'.format(i))
            type = ''.join(type)
            div_content = self.my_xpath(resp, '//table[@class="style team-stat-dsp"]/tbody/tr[{}]/td[1]/div/text()'.format(i))[1]
            div_content = ''.join(div_content).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').split('，排')[0]

            name = self.my_xpath(resp, '//table[@class="style team-stat-dsp"]/tbody/tr[{}]/td[1]/div/a/text()'.format(i))[0]
            rank = self.my_xpath(resp, '//table[@class="style team-stat-dsp"]/tbody/tr[{}]/td[1]/div/span/text()'.format(i))
            rank = ''.join(rank).replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')
            li = [it.get(type),div_content,name,self.get_interger(rank)]
            rank_list.append(li)
        # print(rank_list)
        return rank_list

    def topPlayers(self,resp,teamid,teamName,leagueAbbr):
        items = {
            'goalsPtns':'goals', #进球
            'dribblesPtns':'crossPG', #场均过人
            'passSuccPtns':'passSP',  #传球成功率
            'ratePtns':'rating', #出场 评分
            'shotsPtns':'shotPG', #射门
            'assistsPtns':'assist', #助攻
        }
        data = []
        leagueName = self.my_xpath(resp, '//dl[@id="competition-list"]/dd/a/text()')
        competitionid = self.my_xpath(resp, '//dl[@id="competition-list"]/dd/a/@competitionid')
        seasonYear = self.my_xpath(resp, '//dl[@id="competition-list"]/dd/a/@season')
        for league, com, season in zip(leagueName, competitionid, seasonYear):
            dic = {'leagueName': league}
            url = 'http://www.tzuqiu.cc/playerStatistics/querysPlayerStatPtn.json?competitionId={}&season={}&teamId={}'.format(com, season, teamid)
            res = self.get_data(url)
            try:
                item = json.loads(res)
            except:
                raise Exception('348行的json.loads(),转化失败！')
            for key,value in item.items():
                li_v = []
                if key == 'goalsPtns':
                    for p,i in enumerate(value):
                        if p == 0:
                            name = i.get('playerName')
                            playerId = self.get_player_id(playerName_1=name,teamName=teamName,leagueAbbr=leagueAbbr)
                            v = i.get('value')
                            v1 = i.get('value1')
                            v3 = i.get('value3')
                            li = [playerId,name,v,v1,v3]
                            li_v.append(li)
                        else:
                            v1 = i.get('value1')
                            name = i.get('playerName')
                            playerId = self.get_player_id(playerName_1=name,teamName=teamName,leagueAbbr=leagueAbbr)
                            v = i.get('value')
                            v3 = i.get('value3')
                            if v1 == 0:
                                li = [playerId, name, v, v3]
                            else:
                                li = [playerId, name, v,v1,v3]
                            li_v.append(li)
                elif key == 'ratePtns':
                    for i in value:
                        name = i.get('playerName')
                        playerId = self.get_player_id(playerName_1=name,teamName=teamName,leagueAbbr=leagueAbbr)
                        v = i.get('value')
                        v1 = i.get('value1')
                        v2 = i.get('value2')
                        v3 = i.get('value3')
                        li = [playerId,name,v1,v2,v,v3]
                        li_v.append(li)
                elif key == 'competitionId':
                    continue

                elif key == 'passSuccPtns':
                    for i in value:
                        name = i.get('playerName')
                        playerId = self.get_player_id(playerName_1=name,teamName=teamName,leagueAbbr=leagueAbbr)
                        v = round(i.get('value')/100,3)
                        v1 = i.get('value1')
                        li = [playerId, name, v, v1]
                        li_v.append(li)
                else:
                    for i in value:
                        name = i.get('playerName')
                        playerId = self.get_player_id(playerName_1=name,teamName=teamName,leagueAbbr=leagueAbbr)
                        v = i.get('value')
                        v1 = i.get('value1')
                        li = [playerId, name, v, v1]
                        li_v.append(li)
                dic[items.get(key)] = li_v
            data.append(dic)
        # print(data)
        return data
    def rankHistory(self,resp):
        rank_li = []
        history_lis = re.findall(r"var *leaguePositions *= '(.*?)';",resp)[0]
        if history_lis:
            history = json.loads(history_lis)
            for i in history:
                season = i.get('season')
                rank = i.get('position')
                wins = i.get('wins')
                draws = i.get('draws')
                lose = i.get('losts')
                detail = '{}/{}/{}'.format(wins, draws, lose)
                li = [season, rank, detail]
                rank_li.append(li)
        # print(rank_li)
        return rank_li
        
    def teamStrenth(self,resp,teamName,leagueAbbr):
        rank_li = []
        history_li = re.findall(r"var *teamPositionStrengths *= *(\[.*?\]);", resp)[0]
        if history_li:
            data = json.loads(history_li)
            for i in data:
                detail = i.get('detail')
                li = []
                detail = json.loads(detail)
                for r in detail:
                    r.pop('countryId')
                    r.pop('age')
                    playerName = r.get('playerName')
                    playerId = self.get_player_id(playerName,teamName,leagueAbbr)
                    r['playerId'] = playerId
                    li.append(r)
                    # print(r)
                i['detail'] = li
                rank_li.append(i)
        return rank_li

    def move_bf(self,go):
        zb_li = go.split('%')
        if len(zb_li) == 1:
            zb = self.get_interger(zb_li[0])
        else:
            zb = round(self.get_interger(zb_li[0]) / 100, 3)
        return zb

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

    def re_parse(self,resp):
        res,playerName,playerName_en = resp
        try:
            data = json.loads(re.findall(r'var mvData *= *(\{.*?\}\]\});',res)[0])
        except:
            data = {}
        # print(data)
        return data,playerName,playerName_en

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

    def save_database(self,collect, id, teamId, teamData):
        if teamId:
            note = collect.find_one({'teamId':teamId,'season':teamData.get('season')})
            if not note:
                collect.insert_one(teamData)
            else:
                teamid = note.get('teamId')
                if teamid:  # and not teamData['teamId']
                    teamData['teamId'] = teamid
                collect.update_one({'teamId':teamId,'season':teamData.get('season')},{'$set': teamData})
            logger.info('{}数据下载成功！'.format(id))
        else:
            note = collect.find_one({'teamName':teamData.get('teamName'),'season':teamData.get('season')})
            if not note:
                collect.insert_one(teamData)
            else:
                teamid = note.get('teamId')
                if teamid:  # and not teamData['teamId']
                    teamData['teamId'] = teamid
                collect.update_one({'teamName':teamData.get('teamName'),'season':teamData.get('season')},{'$set': teamData})
            logger.info('{}数据下载成功！'.format(id))
    def saveFile(self,teamidT):
        with open('C:\\sports\\football\\data\\teamid_Tz.json','a') as f:
            f.write(json.dumps(teamidT,ensure_ascii=False)+'/,')

    def teamInfo(self,id,teamId,name,season,leagueAbbr): #TeamAdditionalInfo
        # a = shu
        # try:
        url = 'http://www.tzuqiu.cc/teams/{}/show.do'.format(id)
        resp = self.get_data(url)
        item = self.characteristics(resp,teamId,name,season,leagueAbbr)
        item['rankDetails'] = self.rankDetails(resp)
        item['topPlayers'] = self.topPlayers(resp,id,name,leagueAbbr)
        item['teamPosition'] = self.teamStrenth(resp,name,leagueAbbr)
        item['rankHistory'] = self.rankHistory(resp)
        return item
        # except :
        #     a += 1
        #     if a <= 2:
        #         item = self.teamInfo(teamid,a,teamId)
        #         return item
        #     else:
        #         raise Exception('teamid:{}数据下载失败'.format(teamid))

    def save_teamData(self,id,name,season,leagueAbbr): #保存数据
        teamId = self.get_team_id(name)
        try:
            teamData = self.team_detail(id, teamId, name, season,leagueAbbr)
            self.save_database(self.collection_m, id, teamId, teamData) #TeamMatchDetailData
        except Exception as e:
            logger.info('{}数据处理异常{}'.format(id, e))
            it = {'id':id,'name':name,'season':season,'leagueAbbr':leagueAbbr}
            self.saveFile(it)
            logger.info('MatchDetailData-->teamid:{}数据库储存失败!')

        try:
            teamData = self.teamInfo(id, teamId,name,season,leagueAbbr)
            self.save_database(self.collection_a, id, teamId, teamData) #TeamAdditionalInfo
        except Exception as e:
            logger.info('{}数据处理有异常{}'.format(id, e))
            it = {'id':id,'name':name,'season':season,'leagueAbbr':leagueAbbr}
            self.saveFile(it)
            logger.info('AdditionalInfo-->teamid:{}数据库储存失败!')

    def run(self): #测试代码
        for name,id,season,leagueAbbr in self.get_teamUrl():
            if id != 281:
                teamId = self.get_team_id(name)
                b = self.team_detail(id,teamId,season)
                a = self.teamInfo(id,teamId,name,season,leagueAbbr)
                print(b)
                print(a)

    def spiderTeam(self): #线程爬取
        for name, id, season,leagueAbbr in self.get_teamUrl():
            self.pool.apply_async(self.save_teamData,args=(id,name,season,leagueAbbr))
            # self.save_teamData(id, name, season,leagueAbbr)
        self.pool.close()
        self.pool.join()

    def get_teamUrl(self): #获取所有球队id,name,season
        li = []
        li_name = []
        li_season = []
        li_leagueName = []
        url = 'http://www.tzuqiu.cc/teamStatistics/querys.json?start=0&length=1&extra_param%5BisCurrentSeason%5D=true&extra_param%5BcompetitionRange%5D=all'
        resp = self.getData(url=url,a=self.a).text
        resp = json.loads(resp)
        count = resp.get('recordsTotal')
        url_con = 'http://www.tzuqiu.cc/teamStatistics/querys.json?start=0&length={}&extra_param%5BisCurrentSeason%5D=true&extra_param%5BcompetitionRange%5D=all'.format(count)
        res = self.getData(url=url_con,a=self.a).text
        res = json.loads(res)
        for i in res.get('data'):
            teamId = i.get('teamId')
            teamName = i.get('teamName')
            season = i.get('season')
            leagueName = i.get('competitionName')
            li.append(teamId)
            li_name.append(teamName)
            li_season.append(season)
            li_leagueName.append(leagueName)
        return zip(li_name,li,li_season,li_leagueName)

    def ceshi(self): #测试代码
        url = 'http://www.tzuqiu.cc/teams/{}/show.do'.format(5)
        # url = 'http://www.tzuqiu.cc/competitions/13/show.do'
        resp = self.get_data(url)
        print(self.rankHistory(resp))
    
    def ceshi2(self):
        b = self.team_detail(162,10592, '19/20')
        self.save_database(self.collection_m, 162, 10592, b)
        print(b)

if __name__ == '__main__':
    a = TeamD()
    while True:
        a.spiderTeam()
        x = 0
        with open('C:\\sports\\football\\data\\teamid_Tz.json','r') as f:
            id_str = f.read()
            if id_str:
                id_list = set(id_str.split('/,'))
                for i in id_list:
                    if i != '':
                        d = json.loads(i)
                        a.save_teamData(d.get('id'),d.get('name'),'18/19',d.get('leagueAbbr'))
        time.sleep(60*60*24*3)
        with open('C:\\sports\\football\\data\\teamid_Tz.json','w') as f:
            f.write('')
        
    # a.run()
    # a.ceshi2()
    # id = a.get_teamUrl()
    # print(id)


