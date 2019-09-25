import scrapy
import datetime
from ..items import *
import re
from lxml import etree
from ..items import ZhuqiuSpiderItem

class LeisuSpiderSpider(scrapy.Spider):
    name = 'lei_spider'
    # allowed_domains = ['aaaa']
    start_urls = ['https://data.leisu.com/zuqiu-8437']

    def parse(self, response):
        item_list = []
        for i in range(1, 8):
            item = {}
            match = response.xpath('//div[@class="left-list"]/div[{}]/div[@class="title"]/span/text()'.format(i)).extract_first()

            if i == 1:
                data = []
                data_item = {}
                match_title = response.xpath('//div[@class="left-list"]/div[1]/div[3]/div[@class="title"]/span[@class="txt"]/text()').extract()[0]
                match_ul = response.xpath('//div[@class="left-list"]/div[{}]/div[3]/ul[@class="fd-list"]'.format(i)).extract_first()
                url_list =self.my_xpath(match_ul,'//ul[@class="fd-list"]/li/a/@href')
                seasonid_list = self.my_xpath(match_ul, '//ul[@class="fd-list"]/li/@data-id')
                url_list = ['https://data.leisu.com/'+ url.split('/')[-1] for url in url_list]
                title_list = self.my_xpath(match_ul,'//ul[@class="fd-list"]/li/a/span/text()')
                value = list(zip(seasonid_list,title_list,url_list))
                data_item[match_title] = [value]
                data.append(data_item)
            else:
                data = []
                match_title = response.xpath('//div[@class="left-list"]/div[{}]/div[@class="children"]/div[@class="title"]/span[@class="txt"]/text()'.format(i)).extract()
                match_ul = response.xpath('//div[@class="left-list"]/div[{}]/div[@class="children"]/ul[@class="fd-list"]'.format(i)).extract()
                for title,ul in zip(match_title,match_ul):
                    data_item = {}
                    ul_list = []
                    seasonid_list = self.my_xpath(ul, '//ul[@class="fd-list"]/li/@data-id')
                    url_list = self.my_xpath(ul, '//ul[@class="fd-list"]/li/a/@href')
                    url_list = ['https://data.leisu.com/' + url.split('/')[-1] for url in url_list]
                    title_list = self.my_xpath(ul, '//ul[@class="fd-list"]/li/a/span/text()')
                    value = list(zip(seasonid_list,title_list,url_list))
                    ul_list.append(value)
                    data_item[title] = ul_list
                    data.append(data_item)
            item[match] = data
            item_list.append(item)
            yield item

        for item_1 in item_list:
            for title_1,con_1 in item_1.items():
                for item_2 in con_1:
                    for title_2,con_2 in item_2.items():
                        for item_3 in con_2:
                            for seasonid,title_3,con_3_url in item_3:
                                title_name = '/'.join([title_1,title_2,title_3])
                                yield scrapy.Request(url=con_3_url,meta={'seasonid':seasonid,title_name:con_3_url},dont_filter=True,callback=self.detail_parse)

    def detail_parse(self,response):
        
        detail_item_title_name = list(response.meta.keys())[1]
        data = detail_item_title_name.split('/')
        area = data[0]
        country = data[1]
        leagueName = data[2]
        detail_item_seasonid = list(response.meta.values())[0]
        time_zone_list =  response.xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[1]/div/div/div[3]/div/div/ul/li/a/text()').extract()
        time_url_list =  response.xpath('/html/body/div[1]/div[2]/div/div/div[2]/div[1]/div/div/div[3]/div/div/ul/li/a/@href').extract()
        for time_zone,time_url in zip(time_zone_list,time_url_list):
            detail_item = ZhuqiuSpiderItem()
            # detail_name = '/'.join([detail_item_title_name,time_zone])
            detail_item['leagueId'] = int(detail_item_seasonid)
            detail_item['seasonId'] = int(time_url.split('-')[-1])
            detail_item['area'] = area
            detail_item['country'] = country
            detail_item['leagueName'] = leagueName
            detail_item['seasonYear'] = time_zone
            yield detail_item

    #     for title,url in detail_item.items():
    #         yield scrapy.Request(url=url,meta={'detail_title':title,'target_url':url,'sports':'zhuqiu'},callback=self.target_parse,priority=1)
    #
    # def target_parse(self,response):
    #     item = ZhuqiuSpiderItem()
    #     item['detail_title'] = response.meta.get('detail_title')
    #     item['target_url'] = response.meta.get('target_url')
    #     item['detail_page'] = response.meta.get('data').get('detail_page')
    #     item['saiguo'] = response.meta.get('data').get('saiguo')
    #     item['jifen'] = response.meta.get('data').get('jifen')
    #     item['qiudui'] = response.meta.get('data').get('qiudui')
    #     item['rangqiu'] = response.meta.get('data').get('rangqiu')
    #     item['jinqiushu'] = response.meta.get('data').get('jinqiushu')
    #     item['banquanchang'] = response.meta.get('data').get('banquanchang')
    #     yield item

    def my_xpath(self,data,ruler):
        resp = etree.HTML(data)
        return resp.xpath(ruler)