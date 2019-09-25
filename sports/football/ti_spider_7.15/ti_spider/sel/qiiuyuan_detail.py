import re
from lxml import etree
import time
import json
import requests

class Player:
    def __init__(self,url='https://data.leisu.com/player-12395'):
        self.url = url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
            }
    def get_data(self,url):
        resp = requests.get(url=url,headers=self.headers).text
        return resp

    def re_parse_data(self,data):  #赛程赛果数据
        player_id = self.url.split('-')[-1]
        rule = r'var *DATAID=%s,.*?try *\{.*?ABILITY=(\[.*?\]);'%(player_id)
        res = self.is_empty(re.findall(rule,data,re.S))
        if res:
            result = json.loads(res)
        else:
            result = ''
        return result

    def play_detail(self,resp):
        response = etree.HTML(resp)
        player_item = {}
        player_icon = self.parse_url(self.is_empty(response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/@style')))

        player_name = response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/p[1]/span/text()')
        player_name = ''.join(player_name).strip()
        player_name_zh = response.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/p[2]/span/text()')
        for p,zm in enumerate(player_name_zh):
            if zm == '\xa0':
                player_name_zh.remove('\xa0')
                player_name_zh.insert(p,' ')
        player_name_zh = ''.join(player_name_zh)
        player_worth = response.xpath('//div[@class="team-header"]/div[@class="worth"]/div[@class="mask"]/span/text()')
        player_worth = ''.join(player_worth)
        player_info = response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul[@class="info-data"]/li')
        list1 = []
        if player_info:
            for i in range(1, len(player_info)):
                player_info_detail = response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/ul/li[{}]/span/text()'.format(i))
                player_info_detail = ''.join(player_info_detail)

                list1.append(player_info_detail)
        player_info_detail_url = 'https:'+ self.is_empty(response.xpath('//div[@class="clearfix-row"]/div[@class="bd-box info"]/div/a[@class="moer"]/@href'))
        player_five_mango_stars =  self.re_parse_data(resp)
        player_position_main = response.xpath('//div[@class="lineup-center"]/div[2]/span/text()')
        player_position_main = ' '.join(player_position_main)
        player_position_less = response.xpath('//div[@class="lineup-center"]/div[4]/span/span/text()')
        player_position_less = ' '.join(player_position_less)
        player_goodness = response.xpath('//div[@class="lineup-right"]/div[1]/div[2]/span/text()')
        player_goodness = ' '.join(player_goodness)
        player_weakness = response.xpath('//div[@class="lineup-right"]/div[2]/div[2]/span/text()')
        player_weakness = ' '.join(player_weakness)
        player_transfer_record = response.xpath('//div[@class="page-one clearfix-row"]/div[3]')
        player_transfer_record_list = []
        if player_transfer_record:
            tr = response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr')
            for r in range(1,len(tr)):
                item = {}
                player_transfer_date = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[1]/text()'.format(r+1)))
                player_transfer_nature = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[2]/text()'.format(r+1)))
                player_transfer_fee = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[3]/text()'.format(r+1)))
                player_effective_team = self.is_empty(response.xpath('//div[@class="page-one clearfix-row"]/div[3]/div[2]//tr[{}]/td[4]/span/span[2]/text()'.format(r+1)))
                item['player_transfer_date'] = player_transfer_date
                item['player_transfer_nature'] = player_transfer_nature
                item['player_transfer_fee'] = player_transfer_fee
                item['player_effective_team'] = player_effective_team
                player_transfer_record_list.append(item)

        introduction_to_player = response.xpath('//div[@class="float-right w-304 p-b-60"]/div[2]/div[2]/div[1]/text()')
        player_item['player_icon'] = player_icon
        player_item['player_name'] = player_name
        player_item['player_name_zh'] = player_name_zh
        player_item['player_worth'] = player_worth
        player_item['player_info'] = list1
        player_item['player_info_honor'] = self.player_honor(player_info_detail_url)
        player_item['player_five_mango_stars'] = player_five_mango_stars
        player_item['player_position_main'] = player_position_main
        player_item['player_position_less'] = player_position_less
        player_item['player_goodness'] = player_goodness
        player_item['player_weakness'] = player_weakness
        player_item['player_transfer_record_list'] = player_transfer_record_list
        player_item['introduction_to_player'] = introduction_to_player
        print(player_item)

    def player_honor(self,url):
        res = self.get_data(url)
        response = etree.HTML(res)
        tr = response.xpath('//div[@class="trophy-list-panel"]//tr')
        if tr:
            list_honor = []
            for i in range(1,len(tr)+1):
                td = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td'.format(i))
                if td:
                    for r in range(1,len(td)+1):
                        player_honor_item = {}
                        player_honor_title = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[1]/text()'.format(i,r)))
                        player_honor_icon = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/img/@src'.format(i,r)))
                        player_honor_count = self.is_empty(response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[1]/span/text()'.format(i,r)))
                        player_honor_content_name = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[2]//span[@class="u-name o-hidden float-left text-a-l m-r-5"]/text()'.format(i,r))
                        player_honor_content_time = response.xpath('//div[@class="trophy-list-panel"]//tr[{}]/td[{}]/div[2]/div[2]//span[@class="float-left"]/text()'.format(i,r))
                        player_honor_content = []
                        for m in zip(player_honor_content_name,player_honor_content_time):
                            con = ' '.join(m)
                            player_honor_content.append(con)
                        player_honor_item['player_honor_title'] = player_honor_title
                        player_honor_item['player_honor_icon'] = player_honor_icon
                        player_honor_item['player_honor_count'] = player_honor_count
                        player_honor_item['player_honor_content'] = player_honor_content
                        list_honor.append(player_honor_item)
        else:
            list_honor = ''
        return list_honor

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
        resp = self.get_data(self.url)
        self.play_detail(resp)

if __name__ == '__main__':
    a = Player()
    a.run()
    # print(a.player_honor('https://data.leisu.com/player/honor-12395'))