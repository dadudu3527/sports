from multiprocessing.pool import ThreadPool
import re
import time
import json
import logging

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import redis
import pymongo
from lxml import etree

from config import config

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
        # chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(executable_path='chromedriver1.exe', chrome_options=chrome_options)
        self.mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
        client = pymongo.MongoClient(config)
        self.dbname = client['football']
        self.col_team = self.dbname['team']
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
        time.sleep(3)

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
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        tihuan = {'总积分':'tableDetail','主场积分':'tableDetailHome','客场积分':'tableDetailAway',
                  '半场总积分':'tableDetailHalf','半场主场积分':'tableDetailHomeHalf','半场客场积分':'tableDetailAwayHalf'}
        if not div_list:
            self.parse_data_fenzu(seasonId,leagueId)
        else:
            page = self.driver.page_source
            gg = self.my_xpath(page,'//*[@id="stages-nav"]/a[1]')
            if gg:
                jifen = self.driver.find_element_by_xpath('//*[@id="stages-nav"]/a[1]')
                jifen.click()
                
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
                    # try:
                        # fenzu = self.driver.find_element_by_xpath('//*[@id="stages-nav"]/a[{}]'.format(div))
                        # fenzu.click()
                    # except:
                        # fenzu = ''
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
                        self.collection_jf.update_one({'seasonId': seasonId,'round':items.get('round')}, {'$set': items})
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
                            if str(jinqiu).isdigit():
                                goal = int(jinqiu)
                                penaltyGoal = ''
                            else:
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
                            if str(huang_hongpai).strip().isdigit():
                                yellowCard = int(huang_hongpai)
                                redCard = ''
                            else:
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
                            note = self.col_team.find_one({'name_zh':qiuduiName},{'teamId':1})
                            if note:
                                qiudui = note.get('teamId')
                            else:
                                qiudui = ''
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

    def get_interger(self, data):
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
                if jieguo.isdigit():
                    jieguo = self.get_interger(jieguo)
            else:
                jieguo = ''
        else:
            jieguo = self.get_interger(data)
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

    def parse_data(self,note):
        seasonId = int(note)
        url = 'https://data.leisu.com/zuqiu-{}'.format(seasonId)
        self.get_data(url)
        leagueId = self.getLeagueId(seasonId)
        try:
            self.parse_data_jifen(seasonId,leagueId)
        except Exception as e:
            self.mdb.sadd('errorSeason',note)
            logger.info('{}下载失败！{}'.format(e, note)) 
            
        self.parse_data_qiudui(seasonId,leagueId)
        self.parse_data_rangqiu(seasonId,leagueId)
        self.parse_data_jinqiushu(seasonId,leagueId)
        self.parse_data_banquanchang(seasonId,leagueId)

        #self.exit_chrome()

    def run(self):
        while True:
            print('开始从redis读取。。。')
            note = self.mdb.spop('getSeasonUrl')
            if note and (int(note) != 8796 and int(note) != 8828):
                try:
                    self.parse_data(note)
                except Exception as e:
                    self.mdb.sadd('errorSeason',note)
                    logger.info('{}下载失败！{}'.format(e, note))
            else:
                print('开始睡眠...')
                time.sleep(60*5)
            
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
            

if __name__ == "__main__":
    a = TiyuData()
    a.run()
    a.exit_chrome()
    # a.thread_pool2()