import re
from lxml import etree
import time
import json
import requests
import pymongo
from player import Player
import random
from user_agent import agents
import logging
from config import config

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class Team:
    def __init__(self):
        client = pymongo.MongoClient(config)
        dbname_l = client['football']
        self.collection_league = dbname_l['team']
        self.collection = dbname_l['teamErrorId']
        self.headers = {
            'User-Agent': random.choice(agents)
            }
    def get_data(self,url):
        resp = requests.get(url=url,headers=self.headers,timeout=5).text
        return resp

    def honor(self,team_id):
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

    def detail(self,data):
        response = etree.HTML(data)
        div_list = response.xpath('//div[@class="float-right w-304 p-b-60"]/div')
        team_detail = ''
        if div_list:
            for div in range(1, len(div_list) + 1):
                title_top = self.is_empty(response.xpath(
                    '//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="clearfix-row"]/div[@class="title-tip float-left m-t-16"]/text()'.format(div)))
                if title_top == '球队简介':
                    team_detail = response.xpath(
                        '//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/div[1]/text()'.format(div))
                    team_detail.insert(1,'')
                    team_detail = '\n'.join(team_detail)
                else:
                    pass
        return team_detail

    def detail_page(self,data,team_id): #首页起始数据
        response = etree.HTML(data)
        items = {}
        zhujiaolian = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[1]/div[2]/text()'))
        coachName  = zhujiaolian.split('：')[-1]
        team_worth = response.xpath('//div[@class="team-header"]/div[@class="worth"]/div[@class="mask"]/span/text()')
        team_worth = ''.join(team_worth).split('：')[-1]
        team_info = response.xpath('//div[@class="clearfix-row bd-box info"]/ul[@class="info-data"]/li')
        items['teamId'] = team_id
        items['teamWorth'] = team_worth
        items['coachName'] = coachName
        if team_info:
            for i in range(1, len(team_info)):
                team_info_suosu = response.xpath(
                    '//div[@class="clearfix-row bd-box info"]/ul/li[{}]/span/text()'.format(i))
                if team_info_suosu[0] == '球场:':
                    items['stadium'] = team_info_suosu[-1]
                    break
        else:
            items['stadium'] = 0

        items['honor'] = self.honor(team_id)
        items['detail'] = self.detail(data)
        return  items
    def player_ur(self,data): #球员url
        response = etree.HTML(data)
        resp = response.xpath('//div[@class="page-one clearfix-row"]/div[2]')
        player_url = []
        if resp:
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr')
            for r in range(2,len(tr)+1):
                url = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[2]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if not url:
                    url = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[1]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(r)))
                if url:
                    qiuyuan_detail ='https:' + url
                    player_url.append(qiuyuan_detail)
        return player_url

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

    def parse_url(self,shuju): #用正则处理球员头像的url
        result = self.is_empty(re.findall(r"background-image: *url\('(.*?)'\);*",shuju))
        result = 'https:' + result
        return result

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
    def runteam(self,url):
        self.url = url
        team_id = int(url.split('-')[-1])
        response = self.get_data(url)
        detail_page_info = self.detail_page(response, team_id)
        print(detail_page_info)

    def run_team2(self,url):
        self.url = url
        team_id = int(url.split('-')[-1])
        response = self.get_data(url)
        player_url = self.player_ur(response)
        detail_page_info = self.detail_page(response, team_id)
        Player().thread_pool(player_url,team_id)
        logger.info('id为{}的球队正在数据库下载中....'.format(team_id))
        self.save_database(team_id,detail_page_info)

    def save_database(self,team_id,data):
        team = self.collection_league.find_one({'teamId':team_id })
        if not team:
            self.collection_league.insert_one(data)
            logger.info('id为{}的球队数据库下载完成'.format(team_id))
        else:
            self.collection_league.update_one({'teamId':team_id },{'$set':data})
            logger.info('id为{}的球队数据库更新完成'.format(team_id))

    def thread_pool(self):
        for i in range(10000,52054):
            url = 'https://data.leisu.com/team-'+str(i)
            try:
                self.run_team2(url)
            except Exception as e:
                note = self.collection.find_one({'teamId':i})
                if not note:
                    self.collection.insert_one({'teamId':i})
                else:
                    self.collection.update_one({'teamId':i},{'$set':{'teamId':i}})
                logger.info('{}下载失败！{}'.format(e,i))
        logger.info('爬取结束！')
        
    def get_errorTeam(self):
        getTeamId = self.collection.find()
        for i in getTeamId:
            id = i.get('teamId')
            url = 'https://data.leisu.com/team-'+str(id)
            try:
                self.run_team2(url)
                self.collection.remove({'teamId':id})
            except Exception as e:
                note = self.collection.find_one({'teamId':i})
                if not note:
                    self.collection.insert_one({'teamId':i})
                else:
                    self.collection.update_one({'teamId':i},{'$set':{'teamId':i}})
                logger.info('{}下载失败！{}'.format(e,i))
        logger.info('爬取结束！')

if __name__ == '__main__':
    a = Team() #'https://data.leisu.com/team-40000'
    #a.thread_pool()
    a.get_errorTeam()
    # a.runteam('https://data.leisu.com/team-10009')