import re
from lxml import etree
import time
import json
import re
import requests
import pymongo
from multiprocessing.pool import ThreadPool
from .player import Player
import random
from .user_agent import agents
import logging
from .config import config

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


class Team:
    def __init__(self):
        self.a = 0
        #self.pool = ThreadPool(10)
        client = pymongo.MongoClient(config)
        dbname_l = client['football']
        self.collection_l = dbname_l['league']
        self.collection_league = dbname_l['team']
        self.collection = dbname_l[
            'teamErrorId']  # dbname = client['proxypool']  # collection = dbname['proxies']  # proxies  = collection.find({}).sort('delay')  # self.proxy_list = [proxy['proxy'] for proxy in proxies]

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
        if resp.status_code == 200 and resp.content:
            return resp
        else:
            shu += 1
            if shu <= 10:
                resp = self.getData(url, shu)
                return resp
            else:
                raise Exception('递归次数超过10!')

    def get_data(self, url):
        # headers = {'User-Agent': random.choice(agents)}
        # resp = requests.get(url=url, headers=headers)
        # if resp.status_code == 200:
            # pass
        # else:
        resp = self.getData(url, self.a)
        return resp.text

    def honor(self, team_id):
        resp = self.get_data('https://data.leisu.com/team/honor-{}'.format(team_id))
        response = etree.HTML(resp)
        tr = response.xpath('//div[@class="trophy-list-panel"]//tr')
        list_honor = []
        if tr:
            for i in range(1, len(tr) + 1):
                td = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td'.format(i))
                if td:
                    for r in range(1, len(td) + 1):
                        player_honor_item = {}
                        player_honor_title = self.is_empty(response.xpath(
                            '//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[1]/text()'.format(i, r)))
                        player_honor_icon = self.is_empty(response.xpath(
                            '//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/img/@src'.format(i, r)))
                        player_honor_count = self.is_empty(response.xpath(
                            '//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/span/text()'.format(i, r)))
                        player_honor_content_time = response.xpath(
                            '//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[2]/*[@class="link"]/text()'.format(
                                i, r))
                        player_honor_item['logo'] = player_honor_icon
                        player_honor_item['name'] = player_honor_title
                        player_honor_item['count'] = player_honor_count
                        player_honor_item['detail'] = player_honor_content_time
                        list_honor.append(player_honor_item)
        return list_honor

    def detail(self, data):
        response = etree.HTML(data)
        div_list = response.xpath('//div[@class="float-right w-304 p-b-60"]/div')
        team_detail = ''
        if div_list:
            for div in range(1, len(div_list) + 1):
                title_top = self.is_empty(response.xpath(
                    '//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="clearfix-row"]/div[@class="title-tip float-left m-t-16"]/text()'.format(
                        div)))
                if title_top == '球队简介':
                    team_detail = response.xpath(
                        '//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/div[1]/text()'.format(div))
                    team_detail.insert(1, '')
                    team_detail = '\n'.join(team_detail)
                else:
                    pass
        return team_detail

    def detail_page(self,data,team_id): #首页起始数据
        response = etree.HTML(data)
        dic = {'所属联赛:':'leagueId','球场:':'stadium','成立时间:':'foundYear','容量:':'home_persons'}
        items = {}
        team_picture = self.parse_url(self.is_empty(response.xpath('//div[@class="team-header"]/div[@class="icon"]/@style')))
        team_name = response.xpath('//div[@class="team-header"]/div[@class="name"]/p[@class="zh"]/span/text()')
        team_name_zh = ''.join(team_name)
        team_name_en = response.xpath('//div[@class="team-header"]/div[@class="name"]/p[@class="en"]/span/text()')
        for p,zm in enumerate(team_name_en):
            if zm == '\xa0':
                team_name_en.remove('\xa0')
                team_name_en.insert(p,' ')
                
        team_name_en = ''.join(team_name_en)
        zhujiaolian = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[1]/div[2]/text()'))
        coachName  = zhujiaolian.split('：')[-1]
        team_worth = response.xpath('//div[@class="team-header"]/div[@class="worth"]/div[@class="mask"]/span/text()')
        team_worth = ''.join(team_worth).split('：')[-1]
        team_info = response.xpath('//div[@class="clearfix-row bd-box info"]/ul[@class="info-data"]/li')
        items['teamId'] = team_id
        items['logoUrl'] = team_picture
        items['name_zh'] = team_name_zh
        items['name_en'] = team_name_en
        items['teamWorth'] = team_worth
        items['coachName'] = coachName
        if team_info:
            for i in range(1, len(team_info)):
                team_info_suosu = response.xpath(
                    '//div[@class="clearfix-row bd-box info"]/ul/li[{}]/span/text()'.format(i))
                if team_info_suosu[0] == '所属联赛:':
                    leagueId = self.collection_l.find_one({'short_name_zh':team_info_suosu[-1].strip()},{'leagueId':1}).get('leagueId','')
                    items['leagueId'] = leagueId
                else:
                    items[dic.get(team_info_suosu[0])] = team_info_suosu[-1].strip()
        else:
            items['leagueId'] = ''
            items['stadium'] = ''
            items['foundYear'] = ''
            items['home_persons'] = ''
        list_honor = self.honor(team_id)
        if not list_honor:
            list_honor = self.honor(team_id)
        items['honor'] = list_honor
        items['detail'] = self.detail(data)
        return items

    def player_ur(self, data):  # 球员url
        response = etree.HTML(data)
        resp = response.xpath('//div[@class="page-one clearfix-row"]/div[2]')
        player_name = response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[1]/div[1]/text()')
        player_url = []
        if resp and player_name == '球队阵容':
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr')
            for r in range(2, len(tr) + 1):
                url = self.is_empty(response.xpath(
                    '//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if not url:
                    url = self.is_empty(response.xpath(
                        '//div[@class="page-one clearfix-row"]/div[1]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if url:
                    qiuyuan_detail = 'https:' + url
                    player_url.append(qiuyuan_detail)
        return player_url

    def get_interger(self,data):
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
                jieguo = self.get_interger(jieguo)
            else:
                jieguo = ''
        else:
            jieguo = self.get_interger(data)
        return jieguo

    def parse_url(self, shuju):  # 用正则处理球员头像的url
        result = self.is_empty(re.findall(r"background-image: *url\('(.*?)'\);*", shuju))
        if result:
            result = 'https:' + result
            return result
        else:
            return None

    # def run_team(self,url):
    #     self.url = url
    #     team_id = int(url.split('-')[-1])
    #     response = self.get_data(url)
    #     detail_page_info=self.detail_page(response,team_id)
    #     team_event=self.re_parse_data_saiguo(response,team_id)
    #     team_player,player_url=self.xpath_parse_data_qiuyuan(response,team_id)
    #     team_other=self.parse_data_side(response,team_id)
    # Player().thread_pool(player_url,team_id)
    # self.save_database('detail_page_info',team_id,detail_page_info)
    # self.save_database('team_event',team_id,team_event)
    # self.save_database('team_player',team_id,team_player)
    # self.save_database('team_other',team_id,team_other)

    def runteam(self, url):
        self.url = url
        team_id = int(url.split('-')[-1])
        response = self.get_data(url)
        detail_page_info = self.detail_page(response, team_id)
        print(detail_page_info)

    def run_team2(self, url, team_id):
        try:
            self.url = url
            response = self.get_data(url)
            player_url = self.player_ur(response)
            # detail_page_info = self.detail_page(response, team_id)
            Player().thread_pool(player_url, team_id)
            # logger.info('id为{}的球队正在数据库下载中....'.format(team_id))
            # self.save_database(team_id,detail_page_info)
            logger.info('teamId:{}'.format(team_id))
        except Exception as e:
            note = self.collection.find_one({'teamId': team_id})
            if not note:
                self.collection.insert_one({'teamId': team_id})
            logger.info('{}下载失败！{}'.format(e, i))

    def save_database(self, team_id, data):
        dd = 0
        honor_list = []
        for i in data['honor']:
            logo_url_lei = i.get('logo')
            logo_url = logo_url_lei.replace('http://cdn.leisu.com/honor/','https://cdn.bzsports.com/image/team/honor/')
            resp = self.get_data(logo_url)
            img_src = re.findall(r'<Code>NoSuchKey</Code>',resp)
            if not img_src:
                i['logo'] = logo_url
            honor_list.append(i)   
        data['honor'] = honor_list
        team = self.collection_league.find_one({'teamId': team_id})
        if not team:
            for key,value in data.items():
                if key != 'teamId':
                    if not value:
                        dd = 5
            if dd == 0:
                self.collection_league.insert_one(data)
                logger.info('id为{}的球队数据库下载完成'.format(team_id))
            else:
                raise Exception('数据为空！')
           
        # else:
            # self.collection_league.update_one({'teamId': team_id}, {'$set': data})
            # logger.info('id为{}的球队数据库更新完成'.format(team_id))
        return data

    def logo_url(self, url, teamid):
        logger.info('{}:下载teamId_url'.format(teamid))
        try:
            resp = self.getData(url, self.a)
            response = etree.HTML(resp.text)
            self.collection.remove({'teamId': teamid})
            team_picture = self.parse_url(
                self.is_empty(response.xpath('//div[@class="team-header"]/div[@class="icon"]/@style')))
            if team_picture:
                team = self.collection_league.find_one({'teamId': teamid}, {'teamId': 1, 'logoUrl': 1})
                if team:
                    logo = team.get('logoUrl')
                    if not logo:
                        self.collection_league.update_one({'teamId': teamid}, {'$set': {'logoUrl': team_picture}})
                        logger.info('{}:teamId_url更换成功！'.format(teamid))
        except Exception as e:
            self.collection.insert_one({'teamId': teamid})
            logger.info('{}下载失败！{}'.format(e, teamid))

    # def thread_pool(self):  # 30762
    #     for i in range(11581, 52054):
    #         url = 'https://data.leisu.com/team-' + str(i)
    #         self.pool.apply_async(self.run_team2, args=(url, i))
    #     self.pool.close()
    #     self.pool.join()
    #     logger.info('爬取结束！')

    def crm(self,teamid):
        try:
            url = 'https://data.leisu.com/team-{}'.format(teamid)
            response = self.get_data(url)
            detail_page_info = self.detail_page(response, teamid)
            if not detail_page_info.get('logoUrl'):
                detail_page_info = self.detail_page(response, teamid)
                if not detail_page_info.get('logoUrl'):
                    detail_page_info = self.detail_page(response, teamid)
                    if not detail_page_info.get('logoUrl'):
                        detail_page_info = self.detail_page(response, teamid)
                        if not detail_page_info.get('logoUrl'):
                            detail_page_info = self.detail_page(response, teamid)
                            if not detail_page_info.get('logoUrl'):
                                detail_page_info = self.detail_page(response, teamid)
                            else:
                                raise Exception('403')
            item = self.save_database(teamid,detail_page_info)
            if item.get('_id','f') != 'f':
                item.pop('_id')
        except Exception as e:
            item = {'error':403}
        return item

    # def swap_url(self):  # 30762
    #     for i in range(10000, 52054):
    #         url = 'https://data.leisu.com/team-' + str(i)
    #         self.pool.apply_async(self.logo_url, args=(url, i))
    #     self.pool.close()
    #     self.pool.join()
    #     logger.info('爬取结束！')

    # def get_errorTeam(self):
    #     getTeamId = self.collection.find()
    #     li = []
    #     for i in getTeamId:
    #         id = i.get('teamId')
    #         li.append(id)
    #     for id in li:
    #         url = 'https://data.leisu.com/team-' + str(id)
    #         self.pool.apply_async(self.logo_url, args=(url, id))
    #     self.pool.close()
    #     self.pool.join()
    #     logger.info('爬取结束！')


if __name__ == '__main__':
    a = Team()  # 'https://data.leisu.com/team-40000'
    a.crm(10009)
    # a.get_errorTeam()
    # a.logo_url('https://data.leisu.com/team-10001',10001)
    #  while True:  # getTeamId = a.collection.find()
    #  if getTeamId:
    #  a.get_errorTeam(getTeamId)
    #  else:
    #  break
    #  a.runteam('https://data.leisu.com/team-10009')
    #  a.swap_url()



