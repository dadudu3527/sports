# -*- coding: utf-8 -*-
import scrapy
import datetime
from ..items import *
import re
from lxml import etree
# class Response:
#     @property
#     def url(self):
#         return 'https://live.leisu.com/wanchang?date=20090601'

class TiyuSpiderSpider(scrapy.Spider):
    name = 'tiyu_spider'
    # allowed_domains = ['aaaa']
    start_urls = ['https://live.leisu.com/wanchang?date=20090601']
    def parse(self, response):
        date_start = response.url.split('=')[-1]
        date2 = datetime.datetime.strptime(date_start, '%Y%m%d').date()
        date1 = date2 + datetime.timedelta(days=1)
        date = date1.strftime('%Y%m%d')
        date3 = date2.strftime('%Y-%m-%d')

        resp = response.xpath('//div[@id="finished"]/ul[@class="layout-grid-list"]/li').extract()

        for r in resp:
            items = TiSpiderItem()
            res = etree.HTML(r)
            match_name = res.xpath('//div[@class="clearfix-row"]//span[@class="lab-events"]/a/span/text()')[0].strip()
            icon = res.xpath('//div[@class="clearfix-row"]//span[@class="lab-events"]/span[@class="event-icon"]/@style')[0].strip()
            icon = re.findall(r'background-image:url\((.*?)\);',icon)[0]
            icon_url ='http:{}'.format(icon)
            rotary = res.xpath('//div[@class="clearfix-row"]//span[@class="lab-round"]/text()')[0].strip()
            match_time = res.xpath('//div[@class="clearfix-row"]//span[@class="lab-time"]/text()')[0].rstrip()
            match_datetime = date3 + match_time
            match_status = res.xpath('//div[@class="clearfix-row"]//span[@class="lab-status skin-color-s no-start"]/text()')[0].strip()
            home_team = res.xpath('//div[@class="clearfix-row"]//span[@class="float-left position-r w-300"]/span[@class="lab-team-home"]//a/text()')[0].strip()
            finish_score = res.xpath('//div[@class="clearfix-row"]//span[@class="float-left position-r w-300"]/span[@class="lab-score color-red"]/span[1]/b/text()')[0].strip()
            away_team = res.xpath('//div[@class="clearfix-row"]//span[@class="float-left position-r w-300"]/span[@class="lab-team-away"]//a/text()')[0].strip()
            half_score = res.xpath('//div[@class="clearfix-row"]//span[@class="float-right"]/span[@class="lab-half"]/text()')[0].strip()

            corner = self.analyse(res.xpath('//div[@class="clearfix-row"]//span[@class="float-right"]/span[@class="lab-corner"]/span[1]/text()'), '')
            success_or_failure = res.xpath('//div[@class="clearfix-row"]//span[@class="float-right"]/span[@class="lab-bet-odds"]/span[1]/text()')[0]
            concede_points =  self.analyse(res.xpath('//div[@class="clearfix-row"]//span[@class="float-right"]/span[@class="lab-ratel"]/text()'), '')
            goals_for = self.analyse(res.xpath('//div[@class="clearfix-row"]//span[@class="float-right"]/span[@class="lab-size"]/span[1]/text()'), '')

            items['match_name'] = match_name
            items['icon_url'] = icon_url
            items['rotary'] = rotary
            items['match_time'] = match_time
            items['match_status'] = match_status
            items['home_team'] = home_team
            items['finish_score'] = finish_score
            items['away_team'] = away_team
            items['half_score'] = half_score
            items['corner'] = corner
            items['success_or_failure'] = success_or_failure
            items['concede_points'] = concede_points
            items['goals_for'] = goals_for
            items['match_datetime'] = match_datetime
            yield items

        if date != '20200101' :
            next_zhuqiu_url = 'https://live.leisu.com/wanchang?date=' + date
            yield scrapy.Request(url=next_zhuqiu_url)

        if  date != '20200101' :
            next_lanqiu_url = 'https://live.leisu.com/lanqiu/wanchang?date=' + date_start
            yield scrapy.Request(url=next_lanqiu_url,callback=self.lanqiu_parse)

    def lanqiu_parse(self,response):
        date_start = response.url.split('=')[-1][:4]
        match = response.xpath('//ul[@class="layout-grid-list"]/li').extract()
        for i in match:
            res = etree.HTML(i)
            data = []
            one_item = {}
            two_item = {}
            match_name = res.xpath('//div[@class="list-right"]/div[@class="thead row"]//span[@class="o-hidden lang"]/text()')[0].strip()
            match_time = ' '.join([i.strip() for i in res.xpath('//div[@class ="time-state over"]/span[@class="time"]/text()')])
            match_status = res.xpath('//div[@class ="time-state over"]/span[@class="no-state"]/span/text()')[0].strip()
            match_datetime = date_start + '/' + match_time

            one_team_name = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="team find w-196 color-black"]//span[@class="o-hidden lang"]/text()')[0].strip()
            one_team_icon = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="team find w-196 color-black"]/i[@class="ico"]/@style')[0]
            one_team_icon = 'http:' + re.findall(r'background-image:url\((.*?)\);',one_team_icon)[0]
            one_team_one = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[1]/text()')[0].strip()
            one_team_two = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[2]/text()')[0].strip()
            one_team_three = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[3]/text()')[0].strip()
            one_team_four = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[4]/text()')[0].strip()
            one_team_up_down = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[3]/text()')[0].strip()
            one_team_all_place = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/b/text()')[0].strip()
            one_team_total_difference = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[4]/text()')[0].strip()
            one_team_total_score = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[5]/text()')[0].strip()
            one_team_compensate = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[6]/span[1]/span/text()'), '')
            one_team_let_points1 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[7]/div[1]/text()'), '')
            one_team_let_points2 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[7]/div[2]/span[1]/span/text()'), '')
            one_team_let_points  =  one_team_let_points1 + '  ' + one_team_let_points2
            one_team_all_score1 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[8]/div[1]/text()'), '')
            one_team_all_score2 = self.analyse(res.xpath( '//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-one row"]/div[8]/div[2]/span[1]/span/text()'), '')
            one_team_all_score = one_team_all_score1 + '  ' + one_team_all_score2

            one_item['one_team_name'] = one_team_name
            one_item['one_team_icon'] = one_team_icon
            one_item['one_team_one'] = one_team_one
            one_item['one_team_two'] = one_team_two
            one_item['one_team_three'] = one_team_three
            one_item['one_team_four'] = one_team_four
            one_item['one_team_up_down'] = one_team_up_down
            one_item['one_team_all_place'] = one_team_all_place
            one_item['one_team_total_difference'] = one_team_total_difference
            one_item['one_team_total_score'] = one_team_total_score
            one_item['one_team_compensate'] = one_team_compensate
            one_item['one_team_let_points'] = one_team_let_points
            one_item['one_team_all_score'] = one_team_all_score
            data.append(one_item)

            two_team_name = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="team find w-196 color-black"]//span[@class="o-hidden lang"]/text()')[0].strip()
            two_team_icon = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="team find w-196 color-black"]/i[@class="ico"]/@style')[0]
            two_team_icon = 'http:' + re.findall(r'background-image:url\((.*?)\);', two_team_icon)[0]
            two_team_one = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[1]/text()')[0].strip()
            two_team_two = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[2]/text()')[0].strip()
            two_team_three = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[3]/text()')[0].strip()
            two_team_four = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[@class="race-node find w-167 color-black text-a-c"]/div[4]/text()')[0].strip()
            two_team_up_down = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[3]/text()')[0].strip()
            two_team_all_place = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/b/text()')[0].strip()
            two_team_total_difference = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[4]/text()')[0].strip()
            two_team_total_score = res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[5]/text()')[0].strip()
            two_team_compensate = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[6]/span[1]/span/text()'), '')
            two_team_let_points1 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[7]/div[1]/text()'), '')
            two_team_let_points2 = self.analyse(res.xpath( '//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[7]/div[2]/span[1]/span/text()'), '')
            two_team_let_points = two_team_let_points1 + '  ' + two_team_let_points2
            two_team_all_score1 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[8]/div[1]/text()'), '')
            two_team_all_score2 = self.analyse(res.xpath('//div[@class="list-right"]/div[@class="d-row"]//div[@class="tbody-tow row"]/div[8]/div[2]/span[1]/span/text()'), '')
            two_team_all_score = two_team_all_score1 + '  ' + two_team_all_score2

            two_item['two_team_name'] = two_team_name
            two_item['two_team_icon'] = two_team_icon
            two_item['two_team_one'] = two_team_one
            two_item['two_team_two'] = two_team_two
            two_item['two_team_three'] = two_team_three
            two_item['two_team_four'] = two_team_four
            two_item['two_team_up_down'] = two_team_up_down
            two_item['two_team_all_place'] = two_team_all_place
            two_item['two_team_total_difference'] = two_team_total_difference
            two_item['two_team_total_score'] = two_team_total_score
            two_item['two_team_compensate'] = two_team_compensate
            two_item['two_team_let_points'] = two_team_let_points
            two_item['two_team_all_score'] = two_team_all_score
            data.append(two_item)

            items = LanqiuSpiderItem()
            items['match_name'] = match_name
            items['data'] = data
            items['match_status'] = match_status
            items['match_time'] = match_time
            items['match_datetime'] = match_datetime
            yield items

    def analyse(self,data,str1):
        if data == []:
            data = str1
        else:
            data = data[0].strip()
        return data

# if __name__ == '__main__':
#     a = TiyuSpiderSpider()
#     a.parse(Response())