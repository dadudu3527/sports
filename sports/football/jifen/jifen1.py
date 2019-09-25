from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from config import config
import random
from user_agent import agents
import requests

from multiprocessing.pool import ThreadPool
import pymongo
import re
from lxml import etree
import time
import json
# import requests
import logging

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)
# pool = ThreadPool(2)

class TiyuData:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(executable_path='chromedriver1.exe', chrome_options=chrome_options)
        # self.pool = ThreadPool(1)
        client = pymongo.MongoClient(config)
        self.dbname = client['football']
        self.collection_error = self.dbname['score_error']
        self.col_read = self.dbname['season']
        self.collection_jf = self.dbname['scoreBoard'] #积分榜
        self.collection_fjf = self.dbname['groupScoreBoard'] #分组积分榜
        self.collection_ss = self.dbname['playerGoalTable'] #射手榜
        self.collection_zg = self.dbname['playerAssistTable'] #助攻榜
        self.collection_fs = self.dbname['playerDefensiveTable'] #防守榜
        self.collection_qd = self.dbname['TeamPerformance'] # 球队数据
        self.collection_rt = self.dbname['totalAsiaHandicap'] # 让球总盘路
        self.collection_rh = self.dbname['totalHomeAsiaHandicap'] # 让球主场盘路
        self.collection_ra = self.dbname['totalAwayAsiaHandicap'] # 让球客场盘路
        self.collection_rht = self.dbname['halfAsiaHandicap'] # 让球半场总盘路
        self.collection_rhh = self.dbname['halfHomeAsiaHandicap'] # 让球半场主场盘路
        self.collection_rha = self.dbname['halfAwayAsiaHandicap'] # 让球半场客场盘路
        self.collection_jt = self.dbname['totalGoalLine'] # 进球数总盘路
        self.collection_jh = self.dbname['totalHomeGoalLine'] # 进球数主场盘路
        self.collection_ja = self.dbname['totalAwayGoalLine'] # 进球数客场盘路
        self.collection_jht = self.dbname['halfGoalLine'] # 进球数半场总盘路
        self.collection_jhh = self.dbname['halfHomeGoalLine'] # 进球数半场主场盘路
        self.collection_jha = self.dbname['halfAwayGoalLine'] # 进球数半场客场盘路
        self.collection_bt = self.dbname['halfFullResult'] # 半全场总计
        self.collection_bh = self.dbname['halfFullHomeResult'] # 半全场主场
        self.collection_ba = self.dbname['halfFullAwayResult'] # 半全场客场


    def get_data(self,url):
        self.driver.get(url) # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        time.sleep(1.5)

    def my_xpath(self,data,ruler):
        return etree.HTML(data).xpath(ruler)

    def getLeagueId(self,seasonId):
        note = self.col_read.find_one({'seasonId': seasonId})
        if note:
            leagueId = note.get('leagueId')
        else:
            raise Exception
        return leagueId

    def parse_data_fenzu(self,seasonId,leagueId): #分组积分
        page = self.driver.page_source
        a = False
        #leagueId = self.getLeagueId(seasonId)
        stageId_list = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div/@data-stage')
        stageId_li = list(set(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div/@data-stage')))
        stageId_li.sort(key=stageId_list.index)#set集合去重无序，此方法可以使stageId_li按照set去重之前的顺序排列
        for p,stageId in enumerate(stageId_li):
            stageName = self.is_empty(self.my_xpath(page, '//a[@data-id="{}"]/text()'.format(stageId)))
            items = {'leagueId': leagueId, 'seasonId': seasonId,'stageId':int(stageId),'stageName':stageName}
            groupTable = []
            div_list_now = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(stageId))
            if p == 0:
                for i in range(1,len(div_list_now)+1):  #(len(div_list))
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        for r in range(1,len(tr)):
                            item = {}
                            paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[1]/span/text()'.format(i,r + 1)))
                            qiudui = int(self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[2]/a/@href'.format(i,r + 1))).split('-')[-1])
                            changci = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[3]/text()'.format(i,r + 1)))
                            sheng = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[4]/text()'.format(i,r + 1)))
                            ping = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[5]/text()'.format(i,r + 1)))
                            fu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[6]/text()'.format(i,r + 1)))
                            jinqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[7]/text()'.format(i,r + 1)))
                            shiqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[8]/text()'.format(i,r + 1)))
                            jifen = self.is_empty( self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[13]/text()'.format(i,r + 1)))

                            item['rank'] = paimin
                            item['teamId'] = qiudui
                            item['played'] = changci
                            item['win'] = sheng
                            item['draw'] = ping
                            item['lose'] = fu
                            item['goal'] = jinqiu
                            item['goalAgainst'] = shiqiu
                            item['points'] = jifen
                            rank_List.append(item)
                            if title == '':
                                a = True
                                break
                            else:
                                pass
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            elif p == 1:
                div_list_before = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(stageId_li[p-1]))
                for i in range(len(div_list_before)+1, len(div_list_now)+len(div_list_before) + 1):
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        for r in range(1, len(tr)):
                            item = {}
                            paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[1]/span/text()'.format(i, r + 1)))
                            qiudui = int(self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[2]/a/@href'.format(i, r + 1))).split('-')[-1])
                            changci = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[3]/text()'.format(i, r + 1)))
                            sheng = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[4]/text()'.format(i, r + 1)))
                            ping = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[5]/text()'.format(i, r + 1)))
                            fu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[6]/text()'.format(i, r + 1)))
                            jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[7]/text()'.format(i, r + 1)))
                            shiqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[8]/text()'.format(i, r + 1)))
                            jifen = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[13]/text()'.format(i, r + 1)))

                            item['rank'] = paimin
                            item['teamId'] = qiudui
                            item['played'] = changci
                            item['win'] = sheng
                            item['draw'] = ping
                            item['lose'] = fu
                            item['goal'] = jinqiu
                            item['goalAgainst'] = shiqiu
                            item['points'] = jifen
                            rank_List.append(item)
                            if title == '':
                                a = True
                                break
                            else:
                                pass
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            elif p == 2:
                div_list_before = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(stageId_li[p-1]))
                div_list_before2 = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[@data-stage="{}"]'.format(stageId_li[p-2]))
                for i in range(len(div_list_before)+len(div_list_before2)+1, len(div_list_now)+len(div_list_before)+len(div_list_before2) + 1):
                    # stageId = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-stage'))
                    dic = {}
                    rank_List = []
                    title = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
                    tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
                    if tr == []:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    group = self.is_empty(self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/@data-group'.format(i)))
                    dic['group'] = group
                    if not tr:
                        tr = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
                    if tr:
                        for r in range(1, len(tr)):
                            item = {}
                            paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[1]/span/text()'.format(i, r + 1)))
                            qiudui = int(self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[2]/a/@href'.format(i, r + 1))).split('-')[-1])
                            changci = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[3]/text()'.format(i, r + 1)))
                            sheng = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[4]/text()'.format(i, r + 1)))
                            ping = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[5]/text()'.format(i, r + 1)))
                            fu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[6]/text()'.format(i, r + 1)))
                            jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[7]/text()'.format(i, r + 1)))
                            shiqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[8]/text()'.format(i, r + 1)))
                            jifen = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[13]/text()'.format(i, r + 1)))

                            item['rank'] = paimin
                            item['teamId'] = qiudui
                            item['played'] = changci
                            item['win'] = sheng
                            item['draw'] = ping
                            item['lose'] = fu
                            item['goal'] = jinqiu
                            item['goalAgainst'] = shiqiu
                            item['points'] = jifen
                            rank_List.append(item)
                            if title == '':
                                a = True
                                break
                            else:
                                pass
                        if a:
                            break
                    dic['table'] = rank_List
                    groupTable.append(dic)
            else:
                print('分组赛没有匹配完整！')
            items['groupTable'] = groupTable
            self.save_database(self.collection_fjf,seasonId,items)
            logger.info('seasonId{}:分组积分下载成功'.format(seasonId))


    def parse_data_jifen(self,seasonId,leagueId):  #积分榜
        wait = WebDriverWait(self.driver, 10)
        #leagueId = self.getLeagueId(seasonId)
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        tihuan = {'总积分':'tableDetail','主场积分':'tableDetailHome','客场积分':'tableDetailAway',
                  '半场总积分':'tableDetailHalf','半场主场积分':'tableDetailHomeHalf','半场客场积分':'tableDetailAwayHalf'}
        if not div_list:
            self.parse_data_fenzu(seasonId,leagueId)
        else:
            for y,lun in enumerate(all_score):  #y索引all_score列表
                items ={}
                try:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="match-nav-list m-t-15"]/div[1]/div[1]')))
                    res.click()
                except:
                    res = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/div')))
                    res.click()
                try:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li[{}]/a'.format(y+1)))) #实现轮次的点击
                    lunci.click()
                except:
                    lunci = wait.until(EC.element_to_be_clickable((By.XPATH,'//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li[{}]'.format( y + 1))))  # 实现轮次的点击
                    lunci.click()

                chc = lun.get_attribute('text')
                teamList = []
                items['leagueId'] = leagueId
                items['seasonId'] = seasonId
                if chc != '总积分':
                    items['round'] = int(chc.replace('第','').replace('轮',''))
                else:
                    items['round'] = chc
                for div in range(1,len(div_list)+1):
                    a = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[{}]/div/a'.format(div))
                    try:
                        fenzu = self.driver.find_element_by_xpath('//*[@id="stages-nav"]/a[{}]'.format(div))
                        fenzu.click()
                    except:
                        fenzu = ''
                    for p,i in enumerate(a):
                        time.sleep(0.5)
                        i.click()
                        time.sleep(0.5)
                        jifen_type = i.get_attribute('text')
                        page = self.driver.page_source
                        if p == 0 and y == 0:
                            all_tr = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr')
                            tr = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="data pd-8"]')
                            # tr_w = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="tips data pd-8"]')
                            d_list = []
                            tr_cha = len(all_tr) - len(tr)   #获取tr标签的class属性不是data pd-8 的长度
                            if tr:
                                for r in range(1,len(tr)+tr_cha):
                                    item = {}
                                    qiudui_id = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/@href'.format(r + 1)))
                                    qiudui_id = int(qiudui_id.split('-')[-1])
                                    if jifen_type == '总积分':
                                        qiudui_name = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r+1))).strip()
                                        qiudui_log = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/i/@style'.format(r+1)))
                                        icon_url = 'https:' + re.findall(r"background-image *: *url\('(.*?)'\);", qiudui_log)[0]
                                        team_item = {'teamId':qiudui_id,'teamName':qiudui_name,'teamLogo':icon_url.split('?')[0]}
                                        teamList.append(team_item)

                                    paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[1]/span/text()'.format(r + 1)))
                                    sheng = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                                    ping = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                                    fu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                                    jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                                    shiqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                                    jifen = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[13]/text()'.format(r+1)))
                                    item['rank'] = paimin
                                    item['teamId'] = qiudui_id
                                    item['total'] = len(tr)
                                    item['win'] = sheng
                                    item['draw'] = ping
                                    item['lose'] = fu
                                    item['goal'] = jinqiu
                                    item['goalAgainst'] = shiqiu
                                    item['points'] = jifen
                                    d_list.append(item)
                                if teamList:
                                    items['teamList'] = teamList
                                items[tihuan.get(jifen_type)] = d_list
                            else:
                                print('tr没值')

                        else:
                            d_list = []
                            all_tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr') #获取所有的tr
                            tr_hide = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="data pd-8 hide"]')
                            tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="data pd-8 temporary"]')
                            tr_w = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="tips data pd-8 temporary"]')
                            tr_c = len(all_tr) -  (len(tr) +len(tr_w)) #获取tr标签的class属性不是data pd-8 temporary 以及 tips data pd-8 temporary的长度
                            if tr_hide and tr:
                                for r in range(tr_c+1, len(all_tr)+1):
                                    item = {}
                                    qiudui_id = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/@href'.format(r))).split('-')[-1]
                                    qiudui_id = int(qiudui_id)
                                    
                                    paimin = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[1]/span/text()'.format(r)))
                                    sheng = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[4]/text()'.format(r)))
                                    ping = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[5]/text()'.format(r)))
                                    fu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[6]/text()'.format(r)))
                                    jinqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[7]/text()'.format(r)))
                                    shiqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[8]/text()'.format(r)))
                                    jifen = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[13]/text()'.format(r)))
                                    item['rank'] = paimin
                                    item['teamId'] = qiudui_id
                                    item['win'] = sheng
                                    item['draw'] = ping
                                    item['lose'] = fu
                                    item['goal'] = jinqiu
                                    item['goalAgainst'] = shiqiu
                                    item['points'] = jifen
                                    d_list.append(item)
                                items[tihuan.get(jifen_type)] = d_list
                    data = self.collection_jf.find_one({'seasonId': seasonId,'round':items.get('round')})
                    if not data:
                        self.collection_jf.insert_one(items)
                    else:
                        self.collection_jf.update_one({'seasonId': seasonId}, {'$set': items})
                    logger.info('seasonId{}:积分榜下载成功'.format(seasonId))

    def parse_data_qiudui(self,seasonId,leagueId): #球队球员数据
        # col_item = {'射手榜':'playerGoalTable','助攻榜':'playerAssistTable','球员防守':'playerDefensiveTable','球队数据':'TeamPerformance'}
        #leagueId = self.getLeagueId(seasonId)
        t_item = {'射手榜':'shooterTable','助攻榜':'assistTable','球员防守':'DefendTable','球队数据':'performData'}
        try:
            qiudui_y= self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[2]')
            qiudui_y.click()
        except:
            qiudui_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[1]/a[2]')
            qiudui_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="shooter-list"]/div[1]/a')
        if a_list:
            for p,i in enumerate(a_list):
                items = {'id':p+1,'leagueId':leagueId,'seasonId':seasonId}
                rank_list = []
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                print(name)
                page = self.driver.page_source
                if p == 0:
                    tr = self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[@class="pd-8"]')

                    if tr:
                        for i in range(1,len(tr)+1):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[1]/span/text()'.format(i+1)))
                            qiuyuan_id = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[2]/a/@href'.format(i+1))).split('-')[-1]
                            qiuyuan_id = int(qiuyuan_id)
                            changci_t = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[5]/text()'.format(i+1)))
                            chuchang_time = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[6]/text()'.format(i+1)))
                            jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[7]/text()'.format(i+1)))
                            goal = int(str(jinqiu).split('(')[0].strip())
                            penaltyGoal = int(str(jinqiu).split('(')[-1].split(')')[0])
                            jqhaoshi = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[8]/text()'.format(i+1)))
                            item['rank'] = paiming
                            item['playerId'] = int(qiuyuan_id)
                            item['attendenceNo'] = changci_t
                            item['attandenceMinute'] = chuchang_time
                            item['goal'] = goal
                            item['penaltyGoal'] = penaltyGoal
                            item['goalConsum'] = jqhaoshi
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_ss, seasonId, items)
                    logger.info('seasonId{}:射手榜下载成功'.format(seasonId))

                elif p == 1:
                    tr = self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1,len(tr)+1):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiuyuan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[2]/a/@href'.format(i + 1))).split('-')[-1]
                            chuanqiu_cs = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[5]/text()'.format(i + 1)))
                            guanjcq = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[6]/text()'.format(i + 1)))
                            zhugong = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[7]/text()'.format(i + 1)))
                            item['rank'] = paiming
                            item['playerId'] = int(qiuyuan)
                            item['passNo'] = chuanqiu_cs
                            item['keyPassNo'] = guanjcq
                            item['assistNo'] = zhugong
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_zg, seasonId, items)
                    logger.info('seasonId{}:助攻榜下载成功'.format(seasonId))
                elif p == 2:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1, len(tr) + 1):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiuyuan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[2]/a/@href'.format(i + 1))).split('-')[-1]
                            chuchang_tb = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[5]/text()'.format(i + 1)))
                            qiangduan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[6]/text()'.format(i + 1)))
                            jiewei = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[7]/text()'.format(i + 1)))
                            item['rank'] = paiming
                            item['playerId'] = int(qiuyuan)
                            item['attendenceNo'] = chuchang_tb
                            item['stealNo'] = qiangduan
                            item['clearanceNo'] = jiewei
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_fs, seasonId, items)
                    logger.info('seasonId{}:球员防守榜下载成功'.format(seasonId))

                elif p == 3:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1, len(tr)):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiudui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[2]/a/@href'.format(i + 1))).split('-')[-1]
                            changci = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[3]/text()'.format(i + 1)))
                            jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[4]/text()'.format(i + 1)))
                            shiqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[5]/text()'.format(i + 1)))
                            shemen = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[6]/text()'.format(i + 1)))
                            shezheng = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[7]/text()'.format(i + 1)))
                            dianqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[8]/text()'.format(i + 1)))
                            guanjian_cqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[9]/text()'.format(i + 1)))
                            qiangduan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[10]/text()'.format(i + 1)))
                            jiewei = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[11]/text()'.format(i + 1)))
                            fangui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[12]/text()'.format(i + 1)))
                            huang_hongpai = self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[13]/text()'.format(i + 1))[0]
                            yellowCard = int(huang_hongpai.split('  (')[0])
                            redCard = int(huang_hongpai.split('  (')[-1].split(')')[0])
                            item['rank'] = paiming
                            item['teamId'] = qiudui
                            item['playedNo'] = changci
                            item['goalFor'] = jinqiu
                            item['goalAgainst'] = shiqiu
                            item['shoot'] = shemen
                            item['shootOnTarget'] = shezheng
                            item['penalty'] = dianqiu
                            item['keyPass'] = guanjian_cqiu
                            item['steal'] = qiangduan
                            item['clearance'] = jiewei
                            item['foul'] = fangui
                            item['yellowCard'] = yellowCard
                            item['redCard'] = redCard
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_qd, seasonId, items)
                    logger.info('seasonId{}:球队数据下载成功'.format(seasonId))

    def parse_data_rangqiu(self,seasonId,leagueId):  #让球栏数据匹配
        # col_item = {'总盘路':'totalAsiaHandicap','主场盘路':'totalHomeAsiaHandicap','客场盘路':'totalAwayAsiaHandicap',
        #             '半场总盘路':'halfAsiaHandicap','半场主场盘路':'halfHomeAsiaHandicap','半场客场盘路':'halfAwayAsiaHandicap'}
        #leagueId = self.getLeagueId(seasonId)
        try:
            rangqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[3]')
            rangqiu_y.click()
        except:
            rangqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[3]')
            rangqiu_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="concede-points"]/div/a')
        if a_list:
            for p,i in enumerate(a_list):
                items = {'id': p + 1, 'leagueId': leagueId, 'seasonId': seasonId}
                rank_list = []
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1,len(tr)+1):
                        item = {}
                        paiming = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[1]/span/text()'.format(r+1)))
                        qiudui = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[2]/a/@href'.format(r+1))).split('-')[-1]
                        qiudui = int(qiudui)
                        changci = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        yingpan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        zoupan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        shupan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        shangpan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                        zoupan2 = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[9]/text()'.format(r+1)))
                        xiapan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[10]/text()'.format(r+1)))
                        item['rank'] = paiming
                        item['teamId'] = qiudui
                        item['played'] = changci
                        item['win'] = yingpan
                        item['draw'] = zoupan
                        item['lose'] = shupan
                        item['upTeam'] = shangpan
                        item['normal'] = zoupan2
                        item['downTeam'] = xiapan
                        rank_list.append(item)

                items['handicapTable'] = rank_list
                if name == '总盘路':
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
                logger.info('seasonId{}:让球栏数据匹配{}下载成功'.format(seasonId,name))

    def parse_data_jinqiushu(self,seasonId,leagueId): #进球数栏数据匹配
        #leagueId = self.getLeagueId(seasonId)
        try:
            jinqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[4]')
            jinqiu_y.click()
        except:
            jinqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[4]')
            jinqiu_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="size-page"]/div/a')
        if a_list:
            for p,i in enumerate(a_list):
                items = {'id': p + 1, 'leagueId': leagueId, 'seasonId': seasonId,}
                rank_list = []
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1,len(tr)+1):
                        item = {}
                        paiming = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[1]/span/text()'.format(r+1)))
                        qiuduiId = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[2]/a/@href'.format(r+1))).split('-')[-1]
                        qiuduiName = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r+1)))
                        teamName = self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr/td[2]/a/span/text()'.format(r + 1))
                        teamId = self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr/td[2]/a/@href'.format(r + 1))
                        team_dic = dict(zip(teamName,teamId))
                        if qiuduiId:
                            qiudui = int(qiuduiId)
                        else:
                            qiudui = team_dic.get(qiuduiName,'//data.leisu.com/team-1008611')
                            qiudui = int(qiudui.split('-')[-1])
                        changci = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        daqiu = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        zoupan = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        xiaoqiu = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        item['rank'] = paiming
                        item['teamId'] = qiudui
                        item['played'] = changci
                        item['over'] = daqiu
                        item['draw'] = zoupan
                        item['under'] = xiaoqiu
                        rank_list.append(item)

                items['goalLineTable'] = rank_list
                if name == '总盘路':
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
                logger.info('seasonId{}:进球栏{}下载成功'.format(seasonId,name))

    def parse_data_banquanchang(self,seasonId,leagueId):  #半全场数据匹配
        #leagueId = self.getLeagueId(seasonId)
        try:
            banquanchang_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[5]')
            banquanchang_y.click()
        except:
            banquanchang_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[5]')
            banquanchang_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="double-result"]/div/a')
        if a_list:
            for p,i in enumerate(a_list):
                items = {'id': p + 1, 'leagueId': leagueId, 'seasonId': seasonId,}
                rank_list = []
                time.sleep(1)
                i.click()
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page, '//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1, len(tr) + 1):
                        item = {}
                        qiudui = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[1]/a/@href'.format(r+1))).split('-')[-1]
                        qiudui = int(qiudui)
                        shengsheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[2]/text()'.format(r+1)))
                        shengping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        shengfu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        pingsheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        pingping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        pingfu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                        fusheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                        fuping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[9]/text()'.format(r+1)))
                        fufu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[10]/text()'.format(r+1)))
                        item['teamId'] = qiudui
                        item['ww'] = shengsheng
                        item['wd'] = shengping
                        item['wl'] = shengfu
                        item['dw'] = pingsheng
                        item['dd'] = pingping
                        item['dl'] = pingfu
                        item['lw'] = fusheng
                        item['ld'] = fuping
                        item['ll'] = fufu
                        rank_list.append(item)

                items['halfFullTable'] = rank_list
                if name == '总计':
                    self.save_database(self.collection_bt, seasonId, items)
                elif name == '主场':
                    self.save_database(self.collection_bh, seasonId, items)
                elif name == '客场':
                    self.save_database(self.collection_ba, seasonId, items)
                logger.info('seasonId{}:半全场{}下载成功'.format(seasonId,name))

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

    def exit_chrome(self):
        time.sleep(5)
        self.driver.quit()

    def save_file(self,info): #保存
        with open('E:\\data\\match.txt','a') as f:
            f.write(info)

    def save_database(self,collection,seasonId,items):
        data = collection.find_one({'seasonId': seasonId})
        if not data:
            collection.insert_one(items)
        else:
            collection.update_one({'seasonId': seasonId}, {'$set': items})

    def parse_data(self,url):
        self.get_data(url)
        seasonId = int(url.split('-')[-1])
        leagueId = self.getLeagueId(seasonId)

        try:
            self.parse_data_jifen(seasonId,leagueId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'积分榜'})
            logger.info('积分榜：{}写入失败{}'.format(seasonId,e))
        try:
            self.parse_data_qiudui(seasonId,leagueId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'球队数据'})
            logger.info('球队数据：{}写入失败{}'.format(seasonId,e))
        try:
            self.parse_data_rangqiu(seasonId,leagueId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'让球'})
            logger.info('让球：{}写入失败{}'.format(seasonId,e))
        try:
            self.parse_data_jinqiushu(seasonId,leagueId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'进球数'})
            logger.info('进球数：{}写入失败{}'.format(seasonId,{}))
        try:
            self.parse_data_banquanchang(seasonId,leagueId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'半全场'})
            logger.info('半全场：{}写入失败{}'.format(seasonId,e))

        #self.exit_chrome()
        
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
        
    def current_season(self):
        seasonId_li = []
        seasonId_liu = []
        url = 'https://data.leisu.com/'
        resp = self.getData(url,0).text
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

    def run(self):
        # li = [8925, 8167, 6693, 4942, 658, 657, 656, 655, 654, 653, 652, 651, 650, 649, 8835, 8817, 7101, 5591, 669, 668, 667, 666, 665, 664, 663, 662, 661, 660, 659, 5223, 648, 647, 646, 645, 644, 643, 642, 641, 640, 639, 638, 8584, 7324, 5224, 677, 676, 675, 674, 673, 672, 671, 670, 8551, 7275, 5222, 637, 636, 635, 634, 633, 632, 631, 630, 629, 628, 627, 626, 8019, 5683, 5072, 5071, 5070, 8767, 8020, 5691, 5692, 8733, 7562, 5610, 4979, 4865, 4864, 4863, 4862, 4861, 4860, 8754, 7987, 5690, 5069, 8766, 7560, 5418, 4849, 4848, 4847, 4846, 4845, 4844, 4843, 4842, 4841, 4840, 8614, 7205, 5170, 4832, 4831, 4830, 4829, 4828, 4827, 8737, 4826, 4825, 4824, 4823, 4822, 7566, 5415, 4821, 4820, 4819, 4818, 4817, 4816, 4815, 8615, 7283, 5416, 4839, 4838, 4837, 4836, 4835, 4834, 4833, 8570, 7462, 5419, 4859, 4858, 4857, 4856, 4855, 4854, 4853, 4852, 4851, 4850, 8716, 7473, 5414, 4814, 4813, 4812, 4811, 4810, 8370, 7030, 5068, 4799, 4798, 4797, 4796, 4795, 4794, 8563, 7374, 5413, 4809, 4808, 4807, 4806, 4805, 4804, 4803, 4802, 4801, 4800, 8743, 7542, 5409, 5410, 4769, 4768, 4767, 4766, 8660, 7406, 5412, 4793, 4792, 4791, 4790, 4789, 4788, 8566, 4787, 4786, 7346, 5633, 4765, 4764, 4763, 4762, 4761, 4760, 4759, 4758, 8642, 7377, 5408, 4757, 4756, 4755, 4754, 4753, 7231, 4752, 4751, 8827, 7744, 5637, 4938, 4775, 4774, 4773, 4772, 4771, 4770, 8550, 7398, 5411, 4785, 4784, 4783, 4782, 4781, 4780, 4779, 4778, 4777, 4776, 8775, 7580, 5694, 8781, 6589, 5553, 8582, 7372, 5407, 4750, 4749, 4748, 4747, 4746, 4745, 4744, 4743, 4742, 5552, 9014, 8400, 7156, 5541, 5542, 8238, 5544, 5545, 5546, 9016, 5529, 5530, 5531, 8528, 5499, 5500, 5501, 5502, 5503, 8958, 8055, 5628, 5481, 5482, 5483, 5484, 5485, 5486, 5487, 5488, 5489, 5490, 8982, 6899, 5521, 5522, 5523, 5524, 5528, 8081, 5518, 5519, 5520, 5477, 5478, 5479, 5480, 7595, 5469, 5470, 5471, 5472, 5473, 5474, 5475, 5476, 8857, 8764, 6183, 5430, 5431, 5432, 5433, 5434, 5435, 8738, 7541, 5406, 4667, 4666, 4665, 4664, 4663, 4662, 4661, 4660, 4659, 4658, 4657, 8771, 8014, 5589, 5448, 5449, 5450, 5451, 5452, 5453]
        # li = [5099, 786, 769, 768, 767, 766, 8317, 5101, 806, 811, 810, 809, 785, 784, 783, 765, 764, 763, 805, 804, 803, 802, 808, 807, 782, 781, 762, 801, 800, 799, 780, 779, 778, 8757, 8507, 798, 797, 796, 8768, 777, 776, 775, 774, 7524, 5230, 5231, 761, 7140, 5097, 752, 751, 795, 8386, 7035, 7036, 7037, 760, 759, 758, 750, 749, 748, 747, 7038, 7039, 757, 756, 746, 745, 744, 743, 742, 8699, 7472, 5227, 724, 8687, 723, 722, 721, 7443, 5228, 729, 720, 719, 718, 728, 727, 726, 725, 717, 5752, 8723, 7445, 5225, 682, 681, 680, 679, 678, 8672, 7417, 7544, 5229, 741, 740, 739, 738, 737, 736, 735, 734, 733, 732, 731, 730, 9101, 8552, 7238, 5096, 8651, 707, 706, 705, 7296, 5226, 716, 715, 704, 703, 702, 701, 714, 713, 712, 700, 699, 698, 711, 710, 709, 697, 696, 5700, 708, 9092, 8547, 7193, 5095, 695, 694, 693, 692, 691, 690, 689, 688, 687, 686, 685, 684, 683, 8560, 7274, 5094, 590, 589, 588, 587, 586, 585, 584, 583, 582, 581, 580]
        li,liu = self.current_season()
        print(liu)
        for r in liu:
            index = li.index(r)
            a = li.pop(index)
            url = 'https://data.leisu.com/zuqiu-{}'.format(a)
            self.parse_data(url)
            # self.parse_data_saiguo(i)

        for i in li:
            url = 'https://data.leisu.com/zuqiu-{}'.format(i)
            try:
                # pool.apply_async(self.parse_data, args=(url,))
                self.parse_data(url)
            except Exception as e:
                errorId = self.collection_error.find_one({'seasonId':i})
                if not errorId:
                    self.collection_error.insert_one({'seasonId':i})
                logger.info('{}下载失败！{}'.format(e, i))
                
    def get_que(self,queue):
        while True:
            id = queue.get()
            url = 'https://data.leisu.com/zuqiu-{}'.format(id)
            try:
                # pool.apply_async(self.parse_data, args=(url,))
                self.parse_data(url)
            except Exception as e:
                errorId = self.collection_error.find_one({'seasonId':i})
                if not errorId:
                    self.collection_error.insert_one({'seasonId':i})
                logger.info('{}下载失败！{}'.format(e, i))
            queue.task_done()
            
def crawler():
    a = TiyuData()
    return a


if __name__ == "__main__":
    a = TiyuData()
    a.run()
    a.exit_chrome()
    # a.thread_pool2()