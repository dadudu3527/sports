from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import re
from lxml import etree
import time
import json
import requests

import logging

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

class TiyuData:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1366,768')
        chrome_options.add_argument('--disable-infobars')
        # chrome_options.add_argument('--headless')
        self.driver =webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options)

    def get_data(self,url):
        self.driver.get(url) # https://data.leisu.com/zuqiu-8872  https://data.leisu.com/zuqiu-8794 https://data.leisu.com/zuqiu-8585 https://data.leisu.com/zuqiu-8847
        time.sleep(1.5)
        self.url = url

    def get_id(self):
        id_str = self.url.split('-')[-1]
        id = '1'+ id_str
        return int(id)

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
        item['id_'] = self.get_id()
        item['title'] = title
        item['icon_url'] = icon_url
        item['suosu'] = suosu
        item['qiudui_count'] = qiudui_count
        item['qiuyuan_count'] = qiuyuan_count
        item['feibentu_count'] = feibentu_count
        item['qiuduishizhi'] = qiuduishizhi
        item['year'] = year
        logger.info('获取网页详情{}'.format(item))
        return item

    def parse_data_fenzu(self): #分组积分
        fenzujifen_list = []
        page = self.driver.page_source
        a = False
        div_list = self.my_xpath(page, '//div[@id="scoreboard"]/div[3]/div')
        if div_list:
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
                        item['id_'] = self.get_id()
                        item['小组赛'] = title
                        item['排名'] = paimin
                        item['球队'] = qiudui
                        item['场次'] = changci
                        item['胜'] = sheng
                        item['平'] = ping
                        item['负'] = fu
                        item['进球'] = jinqiu
                        item['失球'] = shiqiu
                        item['净胜球'] = jinshengqiu
                        item['场均进球'] = changjunjq
                        item['场均失球'] = changjunsq
                        item['场均净胜'] = changjunjs
                        item['积分'] = jifen

                        if item['小组赛'] == '':
                            a = True
                            break
                        else:
                            fenzujifen_list.append(item)
                    if a:
                        break
                else:
                    logger.info('获取分组积分失败！{}'.format(self.url))
        else:
            logger.info('获取分组积分失败！{}'.format(self.url))
        return fenzujifen_list

    def parse_data_saiguo(self): #赛程赛果
        saicheng_list = []
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
                    denxi_url = 'https:'+self.is_empty(self.my_xpath(page,'//*[@id="matches"]/table/tbody/tr[{}]/td[8]/a[1]/@href'.format(i+1)))
                    items['id_'] = self.get_id()
                    items['轮次'] = lunci
                    items['时间'] = shijian
                    items['主队'] = zhudui
                    items['比分'] = bifen
                    items['半场比分'] = banchang_bifen
                    items['客队'] = kedui
                    items['让球/全场'] = rangqiu_quan
                    items['让球/半场'] = rangqiu_ban
                    items['进球/全场'] = jinqiu_quan
                    items['进球/半场'] = jinqiu_ban
                    items['分析详情url'] = denxi_url
                    saicheng_list.append(items)
                else:
                    continue
        else:
            logger.info('获取赛程赛果列表失败！{}'.format(self.url))
        return saicheng_list

    def parse_data_jifen(self):  #积分榜
        jifenban_list = []
        wait = WebDriverWait(self.driver, 10)
        all_score = self.driver.find_elements_by_xpath('//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li/a')
        div_list = self.driver.find_elements_by_xpath('//*[@id="scoreboard"]/div[contains(@class,"clearfix-row scoreboard-page scoreboard-")]')
        if not div_list:
            fenzujifen_list = self.parse_data_fenzu()
            return fenzujifen_list
        else:
            for y,lun in enumerate(all_score):  #y索引all_score列表
                res = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="match-nav-list m-t-15"]/div[1]/div[1]')))
                res.click()
                lunci = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@class="table-list"]/div[1]/div/div/div[1]/ul/li[{}]/a'.format(y+1)))) #实现轮次的点击
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
                                    item['id_'] = self.get_id()
                                    if fenzu:
                                        item['分组'] = fenzu.get_attribute('text')
                                    item['轮次'] = chc
                                    item['积分类型'] = jifen_type
                                    item['排名'] = paimin
                                    item['球队'] = qiudui.strip()
                                    item['场次'] = changci
                                    item['胜'] = sheng
                                    item['平'] = ping
                                    item['负'] = fu
                                    item['进球'] = jinqiu
                                    item['失球'] = shiqiu
                                    item['净胜球'] = jinshengqiu
                                    item['场均进球'] = changjunjq
                                    item['场均失球'] = changjunsq
                                    item['场均净胜'] = changjunjs
                                    item['积分'] = jifen
                                    # self.save_file(json.dumps(item,ensure_ascii=False)+'\n')
                                    # f.write(json.dumps(item,ensure_ascii=False)+'\n')
                                    jifenban_list.append(item)
                            else:
                                logger.info('获取积分榜失败！{}'.format(self.url))
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
                                    item['id_'] = self.get_id()
                                    if fenzu:
                                        item['分组'] = fenzu.get_attribute('text')
                                    item['轮次'] = chc
                                    item['积分类型'] = jifen_type
                                    item['排名'] = paimin
                                    item['球队'] = qiudui
                                    item['场次'] = changci
                                    item['胜'] = sheng
                                    item['平'] = ping
                                    item['负'] = fu
                                    item['进球'] = jinqiu
                                    item['失球'] = shiqiu
                                    item['净胜球'] = jinshengqiu
                                    item['场均进球'] = changjunjq
                                    item['场均失球'] = changjunsq
                                    item['场均净胜'] = changjunjs
                                    item['积分'] = jifen
                                    jifenban_list.append(item)
                            else:
                                logger.info('获取分组积分失败！{}'.format(self.url))
            return  jifenban_list                               # self.save_file(json.dumps(item, ensure_ascii=False)+'\n')
                                        # f.write(json.dumps(item, ensure_ascii=False) + '\n')
    def parse_data_qiudui(self): #球队球员数据
        qiuduiyuan_list = []
        try:
            qiudui_y= self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[2]')
            qiudui_y.click()
        except:
            pass
        a_list = self.driver.find_elements_by_xpath('//*[@id="shooter-list"]/div[1]/a')
        if a_list:
            for p,i in enumerate(a_list):
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
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
                            item['id_'] = self.get_id()
                            item['射手榜'] = name
                            item['排名'] = paiming
                            item['球员'] = qiuyuan
                            item['球队'] = qiudui
                            item['国籍'] = guoji
                            item['场次(替补)'] = changci_t
                            item['出场时间'] = chuchang_time
                            item['进球'] = jinqiu
                            item['进球耗时'] = jqhaoshi
                            qiuduiyuan_list.append(item)
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
                            item['id_'] = self.get_id()
                            item['助攻榜'] = name
                            item['排名'] = paiming
                            item['球员'] = qiuyuan
                            item['球队'] = qiudui
                            item['国籍'] = guoji
                            item['传球次数'] = chuanqiu_cs
                            item['关键传球'] = guanjcq
                            item['助攻'] = zhugong
                            qiuduiyuan_list.append(item)
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
                            item['id_'] = self.get_id()
                            item['球员防守'] = name
                            item['排名'] = paiming
                            item['球员'] = qiuyuan
                            item['球队'] = qiudui
                            item['国籍'] = guoji
                            item['出场(替补)'] = chuchang_tb
                            item['抢断'] = qiangduan
                            item['解围'] = jiewei
                            qiuduiyuan_list.append(item)

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
                            item['id_'] = self.get_id()
                            item['球队数据'] = name
                            item['排名'] = paiming
                            item['球队'] = qiudui
                            item['球队详情url'] = 'https:'+qiudui_url
                            item['场次'] = changci
                            item['进球'] = jinqiu
                            item['失球'] = shiqiu
                            item['射门'] = shemen
                            item['射正'] = shezheng
                            item['点球'] = dianqiu
                            item['关键传球'] = guanjian_cqiu
                            item['抢断'] = qiangduan
                            item['解围'] = jiewei
                            item['犯规'] = fangui
                            item['黄牌(红牌)'] = huang_hongpai
                            qiuduiyuan_list.append(item)
        else:
            logger.info('获取球队球员数据失败！{}'.format(self.url))
        return  qiuduiyuan_list
    def parse_data_rangqiu(self):  #让球栏数据匹配
        rangqiu_list = []
        rangqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[3]')
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
                        item['id_'] = self.get_id()
                        item[name] = name
                        item['排名'] = paiming
                        item['球队'] = qiudui
                        item['场次'] = changci
                        item['赢盘'] = yingpan
                        item['走盘1'] = zoupan
                        item['输盘'] = shupan
                        item['净胜盘'] = jinshenpan
                        item['上盘'] = shangpan
                        item['走盘2'] = zoupan2
                        item['下盘'] = xiapan
                        rangqiu_list.append(item)
        else:
            logger.info('获取球队球员数据失败{}'.format(self.url))
        return rangqiu_list

    def parse_data_jinqiushu(self): #进球数栏数据匹配
        jinqiu_list = []
        jinqiu_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[4]')
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
                        item['id_'] = self.get_id()
                        item[name] = name
                        item['排名'] = paiming
                        item['球队'] = qiudui
                        item['场次'] = changci
                        item['大球'] = daqiu
                        item['走盘'] = zoupan
                        item['小球'] = xiaoqiu
                        item['大净胜球'] = dajingshengqiu
                        item['大球率'] = daqiulv
                        jinqiu_list.append(item)
        else:
            logger.info('获取进球数栏数据失败{}'.format(self.url))
        return jinqiu_list

    def parse_data_banquanchang(self):  #半全场数据匹配
        banquanchang_list = []
        banquanchang_y = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[2]/div/div/div[3]/div[1]/a[5]')
        banquanchang_y.click()
        a_list = self.driver.find_elements_by_xpath('//*[@id="double-result"]/div/a')
        if a_list:
            for i in a_list:
                time.sleep(0.5)
                i.click()
                name = i.get_attribute('text')
                print(name)
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
                        item['id_'] = self.get_id()
                        item[name] = name
                        item['球队'] = qiudui
                        item['胜胜'] = shengsheng
                        item['胜平'] = shengping
                        item['胜负'] = shengfu
                        item['平胜'] = pingsheng
                        item['平平'] = pingping
                        item['平负'] = pingfu
                        item['负胜'] = fusheng
                        item['负平'] = fuping
                        item['负负'] = fufu
                        banquanchang_list.append(item)
        else:
            logger.info('获取半全场数据失败{}'.format(self.url))
        return  banquanchang_list

    def is_empty(self,data): #处理数据为空的字段
        if data != []:
            jieguo = data[0].strip()
        else:
            jieguo = ''
        return jieguo
    def exit_chrome(self):
        time.sleep(5)
        self.driver.quit()

    def save_file(self,info): #保存
        with open('E:\\data\\match.txt','a') as f:
            f.write(info)
    def run_sports(self,url):
        self.get_data(url)
        sports_item = {}
        detail_page = self.detail_page()
        saiguo = self.parse_data_saiguo()
        jifen = self.parse_data_jifen()
        qiudui = self.parse_data_qiudui()
        rangqiu = self.parse_data_rangqiu()
        jinqiushu = self.parse_data_jinqiushu()
        banquanchang = self.parse_data_banquanchang()
        sports_item['detail_page'] = detail_page
        sports_item['saiguo'] = saiguo
        sports_item['jifen'] = jifen
        sports_item['qiudui'] = qiudui
        sports_item['rangqiu'] = rangqiu
        sports_item['jinqiushu'] = jinqiushu
        sports_item['banquanchang'] = banquanchang
        self.exit_chrome()
        return sports_item

if __name__ == "__main__":
    a = TiyuData()
    a.run_sports('https://data.leisu.com/zuqiu-8782')
