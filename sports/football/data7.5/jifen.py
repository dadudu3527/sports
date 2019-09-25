from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

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
pool = ThreadPool(2)

class TiyuData:
    def __init__(self):
        self.pool = ThreadPool(1)
        client = pymongo.MongoClient(host='192.168.77.114',port=27017)
        dbname = client['error']
        self.dbname = client['football']
        self.collection_error = dbname['score_error']
        self.col_read = self.dbname['getLeagueId']
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
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        # chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(executable_path='chromedriver1.exe', chrome_options=chrome_options)
        self.driver.get(url) # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        time.sleep(1.5)

    def my_xpath(self,data,ruler):
        return etree.HTML(data).xpath(ruler)

    def getLeagueId(self,seasonId):
        note = self.col_read.find_one({'seasonId': seasonId})
        if note:
            leagueId = note.get('leagueId')
        else:
            leagueId = ''
        return leagueId

    def detail_page(self,seasonId):
        page = self.driver.page_source
        item = {}
        title = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]//div[@class="clearfix-row f-s-24"]/text()'))
        icon_url = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]//div[@class="macth-icon"]/@style'))
        icon_url = 'https:' + re.findall(r"background-image *: *url\('(.*?)'\);",icon_url)[0]
        suosu = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[1]/text()')).split('：')[-1]
        qiudui_count = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[2]/text()')).split('：')[-1]
        qiuyuan_count= self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[4]/text()')).split('：')[-1]
        feibentu_count = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[5]/text()')).split('：')[-1]
        qiuduishizhi = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[6]/text()')).split('：')[-1]
        year = self.is_empty(self.my_xpath(page,'/html/body/div[1]/div[2]/div/div/div[2]/div[1]/div/div/div[3]/div[1]/span/span/text()'))
        item['leagueId'] = ''
        item['seasonId'] = seasonId
        item['title'] = title
        item['icon_url'] = icon_url
        item['belong_league'] = suosu
        item['team_count'] = qiudui_count
        item['player_count'] = qiuyuan_count
        item['NonLocal_player'] = feibentu_count
        item['team_worth'] = qiuduishizhi
        item['year'] = year

        print(item)

    def parse_data_fenzu(self,seasonId): #分组积分
        page = self.driver.page_source
        a = False
        leagueId = self.getLeagueId(seasonId)
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
            print(items)

    def parse_data_saiguo(self): #赛程赛果
        page = self.driver.page_source
        tr_length = self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr')
        seasonYear = self.is_empty(self.my_xpath(page,'//div[@class="select-border"]/span/span/text()'))
        if tr_length:
            for i in range(1,len(tr_length)+1):
                items = {}
                shijian = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/td[2]/span/text()'.format(i + 1))
                shijian = ' '.join(shijian)
                if shijian:
                    matchId = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/@data-id'.format(i+1)))
                    Mode = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/@data-mode'.format(i+1)))
                    round = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/@data-round'.format(i+1)))
                    StageId = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/@data-stage'.format(i+1)))
                    Group = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/@data-group'.format(i+1)))
                    stageName = self.is_empty(self.my_xpath(page, '//a[@data-id="{}"]/text()'.format(StageId)))

                    items['matchId'] = matchId
                    items['mode'] = Mode
                    items['round'] = round
                    items['stageId'] = StageId
                    items['group'] = Group
                    items['stageName'] = stageName
                    items['seasonYear'] = seasonYear
                    items['matchTime'] = shijian
                    print(items)
                else:
                    continue

    def parse_data_jifen(self,seasonId):  #积分榜
        wait = WebDriverWait(self.driver, 10)
        leagueId = self.getLeagueId(seasonId)
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        tihuan = {'总积分':'tableDetail','主场积分':'tableDetailHome','客场积分':'tableDetailAway',
                  '半场总积分':'tableDetailHalf','半场主场积分':'tableDetailHomeHalf','半场客场积分':'tableDetailAwayHalf'}
        if not div_list:
            self.parse_data_fenzu(seasonId)
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
                items['round'] = int(chc.replace('第','').replace('轮',''))
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
                                        team_item = {'teamId':qiudui_id,'teamName':qiudui_name,'teamLogo':icon_url}
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
                    self.save_database(self.collection_jf, seasonId, items)
                    print(items)

    def parse_data_qiudui(self,seasonId): #球队球员数据
        # col_item = {'射手榜':'playerGoalTable','助攻榜':'playerAssistTable','球员防守':'playerDefensiveTable','球队数据':'TeamPerformance'}
        leagueId = self.getLeagueId(seasonId)
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
                            item['rank'] = paiming
                            item['playerId'] = qiuyuan_id
                            item['attendenceNo'] = changci_t
                            item['attandenceMinute'] = chuchang_time
                            item['goal'] = goal
                            item['penaltyGoal'] = penaltyGoal
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_ss, seasonId, items)
                    print(items)

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
                            item['playerId'] = qiuyuan
                            item['passNo'] = chuanqiu_cs
                            item['keyPassNo'] = guanjcq
                            item['assistNo'] = zhugong
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_zg, seasonId, items)
                    print(items)
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
                            item['playerId'] = qiuyuan
                            item['attendenceNo'] = chuchang_tb
                            item['stealNo'] = qiangduan
                            item['clearanceNo'] = jiewei
                            rank_list.append(item)
                    items[t_item.get(name)] = rank_list
                    self.save_database(self.collection_fs, seasonId, items)
                    print(items)

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
                    print(items)

    def parse_data_rangqiu(self,seasonId):  #让球栏数据匹配
        # col_item = {'总盘路':'totalAsiaHandicap','主场盘路':'totalHomeAsiaHandicap','客场盘路':'totalAwayAsiaHandicap',
        #             '半场总盘路':'halfAsiaHandicap','半场主场盘路':'halfHomeAsiaHandicap','半场客场盘路':'halfAwayAsiaHandicap'}
        leagueId = self.getLeagueId(seasonId)
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
                print(items)

    def parse_data_jinqiushu(self,seasonId): #进球数栏数据匹配
        leagueId = self.getLeagueId(seasonId)
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
                print(items)

    def parse_data_banquanchang(self,seasonId):  #半全场数据匹配
        leagueId = self.getLeagueId(seasonId)
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
                print(items)
    def collect(self,collect,id,data_item):
        collection = self.dbname[collect]
        col = collection.find({'id':id})
        if not col:
            collection.insert_one(data_item)

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

    def run(self,url):
        self.get_data(url)
        seasonId = int(url.split('-')[-1])
        # self.detail_page(seasonId)
        # self.parse_data_saiguo()
        try:
            self.parse_data_jifen(seasonId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'积分榜'})
            logger.info('积分榜：{}写入失败'.format(seasonId))
        try:
            self.parse_data_qiudui(seasonId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'球队数据'})
            logger.info('球队数据：{}写入失败'.format(seasonId))
        try:
            self.parse_data_rangqiu(seasonId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'让球'})
            logger.info('让球：{}写入失败'.format(seasonId))
        try:
            self.parse_data_jinqiushu(seasonId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'进球数'})
            logger.info('进球数：{}写入失败'.format(seasonId))
        try:
            self.parse_data_banquanchang(seasonId)
        except Exception as e:
            self.collection_error.insert_one({'seasonId':seasonId,'type':'半全场'})
            logger.info('半全场：{}写入失败'.format(seasonId))

        self.exit_chrome()

    # def thread_pool2(self):
    #     for i in range(2, 9073):
    #         url = 'https://data.leisu.com/zuqiu-{}'.format(i)
    #         try:
    #             self.pool.apply_async(self.run, args=(url,))
    #         except Exception as e:
    #             logger.info('{}下载失败！{}'.format(e, i))
    #     self.pool.close()
    #     self.pool.join()

if __name__ == "__main__":
    # a = TiyuData()# https://data.leisu.com/zuqiu-8679
    # a.run('https://data.leisu.com/zuqiu-6688')
    # a.thread_pool2()
    for i in range(2,9073):
        url = 'https://data.leisu.com/zuqiu-{}'.format(i)
        try:
            pool.apply_async(TiyuData().run, args=(url,))
            # a.run(url)
        except Exception as e:
            logger.info('{}下载失败！{}'.format(e, i))
    pool.close()
    pool.join()