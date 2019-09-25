import re
from lxml import etree
import time
import json
import requests

class Team:
    def __init__(self,url='https://data.leisu.com/team-10009'):
        self.url = url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
            }
    def get_data(self):
        resp = requests.get(url=self.url,headers=self.headers).text
        return resp

    def detail_page(self,data): #首页起始数据
        response = etree.HTML(data)
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
        team_worth = response.xpath('//div[@class="team-header"]/div[@class="worth"]/div[@class="mask"]/span/text()')
        team_worth = ''.join(team_worth)
        team_info = response.xpath('//div[@class="clearfix-row bd-box info"]/ul[@class="info-data"]/li')
        if team_info:
            item = {}
            for i in range(1,len(team_info)):
                team_info_suosu = response.xpath('//div[@class="clearfix-row bd-box info"]/ul/li[{}]/span/text()'.format(i))
                team_info_suosu = ''.join(team_info_suosu)
                item[i] = team_info_suosu
        else:
            item = {'error':'data is null'}

        items['team_picture'] = team_picture
        items['team_name_zh'] = team_name_zh
        items['team_name_en'] = team_name_en
        items['team_worth'] = team_worth
        items['team_info'] = item
        print(items)

    def re_parse_data_saiguo(self,data):  #赛程赛果数据
        team_id = self.url.split('-')[-1]
        rule = r'var *DATAID=%s,.*?try *\{.*?SCHEDULES=(\[.*?\])'%(team_id)
        res = self.is_empty(re.findall(rule,data,re.S))
        if res:
            result = json.loads(res)
            for i in result:
                i.pop('scheduleid')
                i.pop('statusid')
                i.pop('eventid')
                i.pop('seasonid')
                i.pop('season_year')
                print(i)
        else:
            print('没有数据')
    def xpath_parse_data_qiuyuan(self,data): #球员信息
        response = etree.HTML(data)
        resp = response.xpath('//div[@class="page-one clearfix-row"]/div[@class="clearfix-row bd-box m-t-16"]')
        if resp:
            for i in range(1,len(resp)+1):
                if i == 1:
                    item = {}
                    type_name = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[1]/div[1]/text()'.format(i+1)))
                    zhujiaolian = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[1]/div[2]/text()'.format(i+1)))
                    tr = response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr'.format(i+1))
                    for r in range(2,len(tr)+1):
                        haoma = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[1]/text()'.format(i+1,r)))
                        qiuyuan = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/span/text()'.format(i+1,r)))
                        touxiang_url = self.parse_url(self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[1]/@style'.format(i+1,r))))
                        qiuyuan_detail ='https:' + self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(i+1,r)))
                        data_of_birth = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[3]/text()'.format(i+1,r)))
                        shenggao = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[4]/text()'.format(i+1,r)))
                        tizhong = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[5]/text()'.format(i+1,r)))
                        weizi = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[6]/text()'.format(i+1,r)))
                        guoji = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[7]/span/text()'.format(i+1,r)))
                        jinqiu = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[8]/text()'.format(i+1,r)))
                        zhugong = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[9]/text()'.format(i+1,r)))
                        hongpai = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[10]/text()'.format(i+1,r)))
                        huangpai = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[11]/text()'.format(i+1,r)))
                        item['type_name'] = type_name
                        item['zhujiaolian'] = zhujiaolian
                        item['haoma'] = haoma
                        item['qiuyuan'] = qiuyuan
                        item['touxiang_url'] = touxiang_url
                        item['qiuyuan_detail'] = qiuyuan_detail
                        item['data_of_birth'] = data_of_birth
                        item['shenggao'] = shenggao
                        item['tizhong'] = tizhong
                        item['weizi'] = weizi
                        item['guoji'] = guoji
                        item['jinqiu'] = jinqiu
                        item['zhugong'] = zhugong
                        item['hongpai'] = hongpai
                        item['huangpai'] = huangpai
                        print(item)

                elif i == 2:
                    item = {}
                    type_name = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[1]/div[1]/text()'.format(i + 1)))
                    item['type_name'] = type_name
                    tr = response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr'.format(i + 1))
                    for r in range(2, len(tr) + 1):
                        zhuanchu_time = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[1]/text()'.format(i + 1, r)))
                        qiuyuan = self.is_empty(response.xpath( '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/span/text()'.format(i + 1, r)))
                        touxiang_url = self.parse_url(self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[1]/@style'.format(i + 1,r))))
                        qiuyuan_detail = 'https:' + self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(i + 1,r)))
                        weizi = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[3]/text()'.format(i + 1, r)))
                        from_where = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[4]/span/text()'.format(i + 1, r)))
                        type_1 = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[5]/text()'.format(i + 1, r)))

                        item['zhuanru_time'] = zhuanchu_time
                        item['qiuyuan'] = qiuyuan
                        item['touxiang_url'] = touxiang_url
                        item['qiuyuan_detail_url'] = qiuyuan_detail
                        item['weizi'] = weizi
                        item['from_where'] = from_where
                        item['type_1'] = type_1
                        print(item)

                elif i == 3:
                    item = {}
                    type_name = self.is_empty(
                        response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[1]/div[1]/text()'.format(i + 1)))
                    item['type_name'] = type_name
                    tr = response.xpath('//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr'.format(i + 1))
                    for r in range(2, len(tr) + 1):
                        zhuanchu_time = self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[1]/text()'.format(i + 1, r)))
                        qiuyuan = self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/span/text()'.format(i + 1, r)))
                        touxiang_url = self.parse_url(self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[1]/@style'.format(i + 1,r))))
                        qiuyuan_detail = 'https:' + self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[2]/a[2]/@href'.format(i + 1,r)))
                        weizi = self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[3]/text()'.format(i + 1, r)))
                        from_where = self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[4]/span/text()'.format(i + 1,r)))
                        type_1 = self.is_empty(response.xpath(
                            '//div[@class="page-one clearfix-row"]/div[{}]/div[2]//tr[{}]/td[5]/text()'.format(i + 1, r)))

                        item['zhuanchu_time'] = zhuanchu_time
                        item['qiuyuan'] = qiuyuan
                        item['touxiang_url'] = touxiang_url
                        item['qiuyuan_detail_url'] = qiuyuan_detail
                        item['weizi'] = weizi
                        item['to_where'] = from_where
                        item['type_1'] = type_1
                        print(item)
                else:
                    break
        else:
            print('啧啧，啥都没有')
    def parse_data_side(self,data):
        response = etree.HTML(data)
        div_list = response.xpath('//div[@class="float-right w-304 p-b-60"]/div')
        if div_list:
            for div in range(1,len(div_list)+1):
                title_top = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="clearfix-row"]/div[@class="title-tip float-left m-t-16"]/text()'.format(div)))
                if title_top == '进球分布':
                    item = {}
                    jinqiu_type_all = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/a[1]/text()'.format(div)))
                    jinqiu_type_home = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/a[2]/text()'.format(div)))
                    jinqiu_type_away = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/a[3]/text()'.format(div)))
                    mark = response.xpath('//*[@id="goal-all"]/div[1]/div[1]/span/text()')
                    goal_all = response.xpath('//*[@id="goal-all"]/div[1]/div[2]/div/div/span/text()')
                    goal_all_one = self.is_empty(response.xpath('//*[@id="goal-all"]/div[1]/div[2]/div/span/text()'))
                    goal_all.insert(0,goal_all_one)
                    goal_home = response.xpath('//*[@id="goal-home"]/div[1]/div[2]/div/div/span/text()')
                    goal_home_one = self.is_empty(response.xpath('//*[@id="goal-home"]/div[1]/div[2]/div/span/text()'))
                    goal_home.insert(0,goal_home_one)
                    goal_away = response.xpath('//*[@id="goal-away"]/div[1]/div[2]/div/div/span/text()'  )
                    goal_away_one =self.is_empty( response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-away"]/div[@class="score-table"]/div[@class="s-count"]/div/span/text()'.format(div)))
                    goal_away.insert(0,goal_away_one)

                    goal_all_s = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-all"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/div[@class="float-right"]/span/text()'.format(div))
                    goal_all_one_s = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-all"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/span/text()'.format(div)))
                    goal_all_s.insert(0,goal_all_one_s)
                    goal_home_s = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-home"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/div[@class="float-right"]/span/text()'.format(div))
                    goal_home_one_s = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-home"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/span/text()'.format(div)))
                    goal_home_s.insert(0,goal_home_one_s)
                    goal_away_s = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-away"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/div[@class="float-right"]/span/text()'.format(div))
                    goal_away_one_s =self.is_empty( response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[@class="score-table-panel"]/div[@id="goal-away"]/div[@class="score-table"]/div[@class="s-count m-t-4"]/div/span/text()'.format(div)))
                    goal_away_s.insert(0,goal_away_one_s)

                    item['mark'] = mark
                    item[jinqiu_type_all] = {'进球数':goal_all}
                    item[jinqiu_type_home] = {'进球数':goal_home}
                    item[jinqiu_type_away] = {'进球数':goal_away}
                    item[jinqiu_type_all]['失球数'] = goal_all_s
                    item[jinqiu_type_home]['失球数'] = goal_home_s
                    item[jinqiu_type_away]['失球数'] = goal_away_s
                    print(item)
                elif title_top == '球队射手榜':
                    items = {}
                    tr = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/table//tr'.format(div))
                    list_sheshou = []
                    if tr:
                        for r in range(1,len(tr)):
                            item = {}
                            paiming = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/table//tr[{}]/td[1]/text()'.format(div,r+1)))
                            team_name = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]//table/tr[{}]/td[2]/a[2]/span/text()'.format(div,r+1)))
                            bisai = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/table//tr[{}]/td[3]/text()'.format(div,r+1)))
                            jinqiu  = self.is_empty(response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/table//tr[{}]/td[4]/text()'.format(div,r+1)))
                            item['paiming'] = paiming
                            item['team_name'] = team_name
                            item['bisai'] = bisai
                            item['jinqiu'] = jinqiu
                            list_sheshou.append(item)
                    items[title_top] = list_sheshou
                    print(items)
                else:
                    team_detail = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[{}]/div[2]/div[1]/text()'.format(div))
                    team_detail = ''.join(team_detail)
                    item = {'team_detail':team_detail}
                    print(item)

    def is_empty(self,result):
        if result != []:
            jieguo = result[0].strip()
        else:
            jieguo = ''
        return jieguo

    def parse_url(self,shuju): #用正则处理球员头像的url
        result = self.is_empty(re.findall(r"background-image: *url\('(.*?)'\);*",shuju))
        result = 'https:' + result
        return result

    def run(self):
        response = self.get_data()
        self.detail_page(response)
        self.re_parse_data_saiguo(response)
        self.xpath_parse_data_qiuyuan(response)
        self.parse_data_side(response)
if __name__ == '__main__':
    a = Team() #'https://data.leisu.com/team-40000'
    a.run()
