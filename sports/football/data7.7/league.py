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

class TiyuData:
    def __init__(self,url):
        self.pool = ThreadPool(20)
        client = pymongo.MongoClient(host='192.168.77.114', port=27017)
        self.dbname = client['ZHUQIU_SPORTS']
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        # chrome_options.add_argument('--headless')
        self.driver =webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options)
        self.url = url

    def get_data(self):
        self.driver.get(self.url) # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        time.sleep(1.5)

    def my_xpath(self,data,ruler):
        return etree.HTML(data).xpath(ruler)

    def detail_page(self):
        page = self.driver.page_source
        item = {}
        title = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]//div[@class="clearfix-row f-s-24"]/text()'))
        icon_url = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]//div[@class="macth-icon"]/@style'))
        icon_url = 'https:' + re.findall(r"background-image *: *url\('(.*?)'\);",icon_url)[0]
        suosu = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[1]/text()'))
        qiudui_count = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[2]/text()'))
        qiuyuan_count= self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[4]/text()'))
        feibentu_count = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[5]/text()'))
        qiuduishizhi = self.is_empty(self.my_xpath(page,'//div[@class="display-b p-r-30 p-l-30"]/ul[@class="head-list"]/li[6]/text()'))
        year = self.is_empty(self.my_xpath(page,'/html/body/div[1]/div[2]/div/div/div[2]/div[1]/div/div/div[3]/div[1]/span/span/text()'))
        item['title'] = title
        item['icon_url'] = icon_url
        item['belong_league'] = suosu
        item['team_count'] = qiudui_count
        item['player_count'] = qiuyuan_count
        item['NonLocal_player'] = feibentu_count
        item['team_worth'] = qiuduishizhi
        item['year'] = year

        print(item)

    def parse_data_fenzu(self,collection): #分组积分
        page = self.driver.page_source
        a = False
        div_list = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div')
        for i in range(1,len(div_list)+1):
            title = self.is_empty(self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/div/text()'.format(i)))
            tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[class="group"]/div[{}]/table/tbody/tr'.format(i))
            if not tr:
                tr = self.my_xpath(page,'//div[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr'.format(i))
            if tr:
                for r in range(1,len(tr)):
                    item = {}
                    paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[1]/span/text()'.format(i,r + 1)))
                    qiudui = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(i,r + 1)))
                    changci = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[3]/text()'.format(i,r + 1)))
                    sheng = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[4]/text()'.format(i,r + 1)))
                    ping = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[5]/text()'.format(i,r + 1)))
                    fu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[6]/text()'.format(i,r + 1)))
                    jinqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[7]/text()'.format(i,r + 1)))
                    shiqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[8]/text()'.format(i,r + 1)))
                    jinshengqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[9]/text()'.format(i,r + 1)))
                    changjunjq = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[10]/text()'.format(i,r + 1)))
                    changjunsq = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[11]/text()'.format(i,r + 1)))
                    changjunjs = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[12]/text()'.format(i,r + 1)))
                    jifen = self.is_empty( self.my_xpath(page, '//*[@id="scoreboard"]/div[3]/div[{}]/table/tbody/tr[{}]/td[13]/text()'.format(i,r + 1)))
                    # item['轮次'] = lunci
                    item['group_match'] = title
                    item['rank'] = paimin
                    item['team'] = qiudui
                    item['sitting'] = changci
                    item['win'] = sheng
                    item['flat'] = ping
                    item['lose'] = fu
                    item['goal_in'] = jinqiu
                    item['no_ball'] = shiqiu
                    item['goal_difference'] = jinshengqiu
                    item['average_in_goal'] = changjunjq
                    item['average_loss_goal'] = changjunsq
                    item['net_win_per_game'] = changjunjs
                    item['accumulate_points'] = jifen

                    if item['group_match'] == '':
                        a = True
                        break
                    else:
                        print(item)
                if a:
                    break
        return div_list

    def parse_data_saiguo(self): #赛程赛果
        collection = self.dbname['saiguo']
        page = self.driver.page_source
        tr_length = self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr')
        if tr_length:
            for i in range(len(tr_length)):
                if i==0:
                    continue
                items = {}
                shijian = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/td[2]/span/text()'.format(i + 1))
                shijian = ' '.join(shijian)
                if shijian:
                    lunci = self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[1]/span/text()'.format(i+1))
                    if lunci == []:
                        lunci = self.is_empty(self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr[{}]/td[1]/text()'.format(i + 1)))
                    else:
                        lunci = self.is_empty(lunci)
                    zhudui = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[3]/a/text()'.format(i+1)))
                    zhudui2 = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[3]/a/span/text()'.format(i+1)))
                    zhudui = ''.join([zhudui,zhudui2])
                    bifen = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[4]/a/text()'.format(i+1)))
                    banchang_bifen = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[4]/a/span/text()'.format(i+1)))
                    kedui = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[5]/a/text()'.format(i+1)))
                    kedui2 = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[5]/a/span/text()'.format(i+1)))
                    kedui = ''.join([kedui,kedui2])
                    rangqiu_quan = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[6]/div[1]/text()'.format(i+1)))
                    rangqiu_ban = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[6]/div[2]/text()'.format(i+1)))
                    jinqiu_quan = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[7]/div[1]/text()'.format(i+1)))
                    jinqiu_ban = self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[7]/div[2]/text()'.format(i+1)))
                    # denxi_url = 'https:'+self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[8]/a[1]/@href'.format(i+1)))
                    matchId = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr{}/@data-id'.format(i+1))
                    dataMode = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr{}/@data-mode'.format(i+1))
                    round = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr{}/@data-round'.format(i+1))
                    dataStage = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr{}/@data-stage'.format(i+1))
                    dataGroup = self.my_xpath(page, '//*[@id="matches"]/table/tbody/tr{}/@data-group'.format(i+1))
                    stageName = self.my_xpath(page, '//a/[@data-id="{}"]'.format(dataStage))

                    items['round'] = lunci
                    items['match_time'] = shijian
                    items['home_team'] = zhudui
                    items['score'] = bifen
                    items['half_score'] = banchang_bifen
                    items['away_team'] = kedui
                    items['concede_points_all'] = rangqiu_quan
                    items['concede_points_half'] = rangqiu_ban
                    items['goal_in_all'] = jinqiu_quan
                    items['goal_in_half'] = jinqiu_ban
                    # items['分析详情url'] = denxi_url
                    print(items)
                else:
                    continue

    def parse_data_jifen(self):  #积分榜
        collection = self.dbname['jifen']
        wait = WebDriverWait(self.driver, 10)
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        if not div_list:
            self.parse_data_fenzu(collection)
        else:
            for y,lun in enumerate(all_score):  #y索引all_score列表
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
                            tr_cha = len(all_tr) - len(tr)   #获取tr标签的class属性不是data pd-8 的长度
                            if tr:
                                for r in range(1,len(tr)+tr_cha):
                                    item = {}
                                    paimin = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[1]/span/text()'.format(r+1)))
                                    qiudui = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r+1)))
                                    changci = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                                    sheng = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                                    ping = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                                    fu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                                    jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                                    shiqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                                    jinshengqiu = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[9]/text()'.format(r+1)))
                                    changjunjq = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[10]/text()'.format(r+1)))
                                    changjunsq =self.is_empty( self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[11]/text()'.format(r+1)))
                                    changjunjs = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[12]/text()'.format(r+1)))
                                    jifen = self.is_empty(self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[13]/text()'.format(r+1)))
                                    if fenzu:
                                        item['group'] = fenzu.get_attribute('text')
                                    item['round'] = chc
                                    item['score_type'] = jifen_type
                                    item['rank'] = paimin
                                    item['team'] = qiudui.strip()
                                    item['sitting'] = changci
                                    item['win'] = sheng
                                    item['flat'] = ping
                                    item['lose'] = fu
                                    item['goal_in'] = jinqiu
                                    item['no_ball'] = shiqiu
                                    item['goal_difference'] = jinshengqiu
                                    item['average_in_goal'] = changjunjq
                                    item['average_loss_goal'] = changjunsq
                                    item['net_win_per_game'] = changjunjs
                                    item['accumulate_points'] = jifen
                                    print(item)
                            else:
                                print('tr没值')
                        else:
                            all_tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr') #获取所有的tr
                            tr_hide = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="data pd-8 hide"]')
                            tr = self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="data pd-8 temporary"]')
                            tr_w = self.my_xpath(page,'//*[@id="scoreboard"]/div[1]/table/tbody/tr[@class="tips data pd-8 temporary"]')
                            tr_c = len(all_tr) -  (len(tr) +len(tr_w)) #获取tr标签的class属性不是data pd-8 temporary 以及 tips data pd-8 temporary的长度
                            if tr_hide and tr:
                                for r in range(tr_c+1, len(all_tr)+1):
                                    item = {}
                                    paimin = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[1]/span/text()'.format(r)))
                                    qiudui = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r)))
                                    changci =self.is_empty( self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[3]/text()'.format(r)))
                                    sheng = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[4]/text()'.format(r)))
                                    ping = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[5]/text()'.format(r)))
                                    fu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[6]/text()'.format(r)))
                                    jinqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[7]/text()'.format(r)))
                                    shiqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[8]/text()'.format(r)))
                                    jinshengqiu = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[9]/text()'.format(r)))
                                    changjunjq = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[10]/text()'.format(r)))
                                    changjunsq = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[11]/text()'.format(r)))
                                    changjunjs = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[12]/text()'.format(r)))
                                    jifen = self.is_empty(self.my_xpath(page, '//*[@id="scoreboard"]/div[1]/table/tbody/tr[{}]/td[13]/text()'.format(r)))
                                    # item['轮次'] = lunci
                                    if fenzu:
                                        item['group'] = fenzu.get_attribute('text')
                                    item['round'] = chc
                                    item['score_type'] = jifen_type
                                    item['rank'] = paimin
                                    item['team'] = qiudui
                                    item['sitting'] = changci
                                    item['win'] = sheng
                                    item['flat'] = ping
                                    item['lose'] = fu
                                    item['goal_in'] = jinqiu
                                    item['no_ball'] = shiqiu
                                    item['goal_difference'] = jinshengqiu
                                    item['average_in_goal'] = changjunjq
                                    item['average_loss_goal'] = changjunsq
                                    item['net_win_per_game'] = changjunjs
                                    item['accumulate_points'] = jifen
                                    print(item)

    def parse_data_qiudui(self): #球队球员数据
        try:
            qiudui_y= self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[2]')
            qiudui_y.click()
        except:
            qiudui_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[1]/a[2]')
            qiudui_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="shooter-list"]/div[1]/a')
        if a_list:
            for p,i in enumerate(a_list):
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
                            qiuyuan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(i+1)))
                            qiudui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[3]/a/span/text()'.format(i+1)))
                            guoji = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[4]/span/text()'.format(i+1)))
                            changci_t = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[5]/text()'.format(i+1)))
                            chuchang_time = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[6]/text()'.format(i+1)))
                            jinqiu = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[7]/text()'.format(i+1)))
                            jqhaoshi = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[2]/table/tbody/tr[{}]/td[8]/text()'.format(i+1)))
                            item['Top_scorer_list'] = name
                            item['rank'] = paiming
                            item['player'] = qiuyuan
                            item['team'] = qiudui
                            item['country'] = guoji
                            item['sitting_alternate'] = changci_t
                            item['appearance_time'] = chuchang_time
                            item['goal_in'] = jinqiu
                            item['goal_time_consuming'] = jqhaoshi
                            print(item)
                elif p == 1:
                    tr = self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1,len(tr)+1):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiuyuan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(i + 1)))
                            qiudui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[3]/a/span/text()'.format(i + 1)))
                            guoji = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[4]/span/text()'.format(i + 1)))
                            chuanqiu_cs = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[5]/text()'.format(i + 1)))
                            guanjcq = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[6]/text()'.format(i + 1)))
                            zhugong = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[3]/table/tbody/tr[{}]/td[7]/text()'.format(i + 1)))
                            item['Auxiliary_attack_list'] = name
                            item['rank'] = paiming
                            item['player'] = qiuyuan
                            item['team'] = qiudui
                            item['country'] = guoji
                            item['Number_of_passes'] = chuanqiu_cs
                            item['key_pass'] = guanjcq
                            item['assist'] = zhugong
                            print(item)
                elif p == 2:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[4]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1, len(tr) + 1):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiuyuan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(i + 1)))
                            qiudui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[3]/a/span/text()'.format(i + 1)))
                            guoji = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[4]/span/text()'.format(i + 1)))
                            chuchang_tb = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[5]/text()'.format(i + 1)))
                            qiangduan = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[6]/text()'.format(i + 1)))
                            jiewei = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[4]/table/tbody/tr[{}]/td[7]/text()'.format(i + 1)))
                            item['Player_defence'] = name
                            item['rank'] = paiming
                            item['player'] = qiuyuan
                            item['team'] = qiudui
                            item['country'] = guoji
                            item['sitting_alternate'] = chuchang_tb
                            item['steal'] = qiangduan
                            item['debarrass'] = jiewei
                            print(item)

                elif p == 3:
                    tr = self.my_xpath(page, '//*[@id="shooter-list"]/div[5]/table/tbody/tr[@class="pd-8"]')
                    if tr:
                        for i in range(1, len(tr)):
                            item = {}
                            paiming = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[1]/span/text()'.format(i + 1)))
                            qiudui = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(i + 1)))
                            qiudui_url = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[2]/a/@href'.format(i + 1)))
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
                            huang_hongpai = self.is_empty(self.my_xpath(page,'//*[@id="shooter-list"]/div[5]/table/tbody/tr[{}]/td[13]/text()'.format(i + 1)))
                            item['team_data'] = name
                            item['rank'] = paiming
                            item['team'] = qiudui
                            item['team_detail_url'] = 'https:'+qiudui_url
                            item['sitting'] = changci
                            item['goal_in'] = jinqiu
                            item['no_ball'] = shiqiu
                            item['shoot'] = shemen
                            item['shoot_main'] = shezheng
                            item['penalty'] = dianqiu
                            item['key_pass'] = guanjian_cqiu
                            item['steal'] = qiangduan
                            item['debarrass'] = jiewei
                            item['against_rule'] = fangui
                            item['yellow_red_card'] = huang_hongpai
                            print(item)

    def parse_data_rangqiu(self):  #让球栏数据匹配
        try:
            rangqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[3]')
            rangqiu_y.click()
        except:
            rangqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[3]')
            rangqiu_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="concede-points"]/div/a')
        if a_list:
            for i in a_list:
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1,len(tr)+1):
                        item = {}
                        paiming = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[1]/span/text()'.format(r+1)))
                        qiudui = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r+1)))
                        changci = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        yingpan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        zoupan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        shupan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        jinshenpan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                        shangpan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                        zoupan2 = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[9]/text()'.format(r+1)))
                        xiapan = self.is_empty(self.my_xpath(page,'//*[@id="concede-points"]/table/tbody/tr[{}]/td[10]/text()'.format(r+1)))
                        item['type'] = name
                        item['rank'] = paiming
                        item['team'] = qiudui
                        item['sitting'] = changci
                        item['win_the_offer'] = yingpan
                        item['walking_disk_one'] = zoupan
                        item['transmission_plate'] = shupan
                        item['net_winning_offer'] = jinshenpan
                        item['hanging_wall'] = shangpan
                        item['walking_disk_two'] = zoupan2
                        item['foot_wall'] = xiapan
                        print(item)

    def parse_data_jinqiushu(self): #进球数栏数据匹配
        try:
            jinqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[4]')
            jinqiu_y.click()
        except:
            jinqiu_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[4]')
            jinqiu_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="size-page"]/div/a')
        if a_list:
            for i in a_list:
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                print(name)
                page = self.driver.page_source
                tr = self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1,len(tr)+1):
                        item = {}
                        paiming = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[1]/span/text()'.format(r+1)))
                        qiudui = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[2]/a/span/text()'.format(r+1)))
                        changci = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        daqiu = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        zoupan = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        xiaoqiu = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        dajingshengqiu = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                        daqiulv = self.is_empty(self.my_xpath(page,'//*[@id="size-page"]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                        item['type'] = name
                        item['rank'] = paiming
                        item['team'] = qiudui
                        item['sitting'] = changci
                        item['large_ball'] = daqiu
                        item['walking_disk'] = zoupan
                        item['bobble'] = xiaoqiu
                        item['big_net_winning_ball'] = dajingshengqiu
                        item['ball_rate'] = daqiulv
                        print(item)

    def parse_data_banquanchang(self):  #半全场数据匹配
        try:
            banquanchang_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[5]')
            banquanchang_y.click()
        except:
            banquanchang_y = self.driver.find_element_by_xpath('//div[@class="match-nav-list"]/a[5]')
            banquanchang_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="double-result"]/div/a')
        if a_list:
            for i in a_list:
                time.sleep(1)
                i.click()
                name = i.get_attribute('text')
                page = self.driver.page_source
                tr = self.my_xpath(page, '//*[@id="double-result"]/table/tbody/tr[@class="data pd-8 temporary"]')
                if tr:
                    for r in range(1, len(tr) + 1):
                        item = {}
                        qiudui = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[1]/a/span/text()'.format(r+1)))
                        shengsheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[2]/text()'.format(r+1)))
                        shengping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[3]/text()'.format(r+1)))
                        shengfu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[4]/text()'.format(r+1)))
                        pingsheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[5]/text()'.format(r+1)))
                        pingping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[6]/text()'.format(r+1)))
                        pingfu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[7]/text()'.format(r+1)))
                        fusheng = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[8]/text()'.format(r+1)))
                        fuping = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[9]/text()'.format(r+1)))
                        fufu = self.is_empty(self.my_xpath(page,'//*[@id="double-result"]/table/tbody/tr[{}]/td[10]/text()'.format(r+1)))
                        item['type'] = name
                        item['team'] = qiudui
                        item['win_win'] = shengsheng
                        item['win_flat'] = shengping
                        item['win_lose'] = shengfu
                        item['flat_win'] = pingsheng
                        item['flat_flat'] = pingping
                        item['flat_lose'] = pingfu
                        item['lose_win'] = fusheng
                        item['lose_flat'] = fuping
                        item['lose_lose'] = fufu
                        print(item)
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

    def run(self):
        self.get_data()
        self.detail_page()
        self.parse_data_saiguo()
        self.parse_data_jifen()
        self.parse_data_qiudui()
        self.parse_data_rangqiu()
        self.parse_data_jinqiushu()
        self.parse_data_banquanchang()
        self.exit_chrome()

if __name__ == "__main__":
    a = TiyuData('https://data.leisu.com/zuqiu-8782')# https://data.leisu.com/zuqiu-8679
    a.run()
