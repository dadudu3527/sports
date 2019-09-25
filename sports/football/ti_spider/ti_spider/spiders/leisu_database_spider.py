import scrapy
import datetime
from ..items import *
import re
from lxml import etree
from ..items import ZhuqiuSpiderItem

class LeisuSpiderSpider(scrapy.Spider):
    name = 'leisu_spider'
    # allowed_domains = ['aaaa']
    start_urls = ['https://data.leisu.com/zuqiu-8437']

    def parse(self, response):
        item_list = []
        for i in range(1,8):
            item = {}
            match1 = response.xpath('//div[@class="left-list"]/div[{}]'.format(i)).extract()[0]
            match2 = response.xpath('//div[@class="left-list"]/div[{}]'.format(i)).extract()[0]
            resp = etree.HTML(match1)
            match_item_top = resp.xpath('//div[@class="title"]/span/text()')[0]
            if i == 1:
                list_k = []
                res = etree.HTML(match1)
                res = res.xpath('//div/div[3]') #获得热门赛事的div列表
                item_content = {}
                match_item = res.xpath('/div/div[@class="title"]/span/text()')
                target_url_list = res.xpath('/div/div/ul[@class="fd-list"]/li/a/@href')
                target_name_list = res.xpath('/div/div/ul[@class="fd-list"]/li/a/span/text()')
                item_content[match_item] = dict(zip(target_name_list,target_url_list))
                list_k.append(item_content)
            else:
                list_k = []
                res = etree.HTML(match1)
                res = res.xpath('//div[@class="children"]')  # 获得热门赛事的div列表
                for r in res:
                    item_content = {}
                    res = etree.HTML(etree.tostring(r, encoding='utf-8').decode())
                    match_item = res.xpath('/div/div[@class="title"]/span/text()')
                    target_url_list = res.xpath('/div/div/ul[@class="fd-list"]/li/a/@href')
                    target_name_list = res.xpath('/div/div/ul[@class="fd-list"]/li/a/span/text()')
                    item_content[match_item] = dict(zip(target_name_list, target_url_list))
                    list_k.append(item_content)

            item[match_item_top] = list_k
            yield item








        # remen_urls = response.xpath("/html/body/div[1]/div[2]/div/div/div[1]/div/div[1]/div[3]/ul/li/a/@href").extract()
        # guoji_urls = urls = response.xpath("/html/body/div[1]/div[2]/div/div/div[1]/div/div[2]/div[2]/ul/li/a/@href").extract()
        # shatan_urls = response.xpath("/html/body/div[1]/div[2]/div/div/div[1]/div/div[2]/div[3]/ul/li/a/@href").extract()
        # ou_zhou_urls = response.xpath("/html/body/div[1]/div[2]/div/div/div[1]/div/div[3]/div[2]/ul/li/a/@href").extract()
        # England = response.xpath("/html/body/div[1]/div[2]/div/div/div[1]/div/div[3]/div[3]/ul/li/a/@href").extract()
