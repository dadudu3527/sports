# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import pymongo
from scrapy import signals
# from .selenium_tiyu import TiyuData
import scrapy
import random


class TiSpiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        
        
class HttpProxyMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        # request.dont_filter = False
            # client = pymongo.MongoClient(host='192.168.77.114',port=27017)
            # dbname = client['proxypool']
            # collection = dbname['proxies']
            # proxies = collection.find({})
            # proxy =  random.choice([proxy['proxy'] for proxy in proxies])
            # proxies = {
            #     'http': 'http://' + proxy,
            #     'https': 'http://' + proxy
            # }
            # request.meta['proxy'] = proxies
        #     url = request.url
        #     data = self.run_sports(url)
        #     request.meta['data'] = data
        #     return scrapy.http.HtmlResponse(url=request.url, encoding='utf-8',request=request)
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "H875FZ3594Z343AD"
        proxyPass = "9990096723915515"

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {"host": proxyHost, "port": proxyPort, "user": proxyUser,
                                                                    "pass": proxyPass, }

        proxies = {"http": proxyMeta, "https": proxyMeta, }
        # url = request.url
        # if url.startswith("http://"):
            # proxies = "http://"+str(proxy)
        # elif url.startswith("https://"):
            # proxies = "https://"+str(proxy)
        #注意这里面的meta={'proxy':proxies},一定要是proxy进行携带,其它的不行,后面的proxies一定 要是字符串,其它任何形式都不行
        request.meta['proxy'] = proxyMeta
        
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

    # def run_sports(self,url):
    #     self.get_data(url)
    #     sports_item = {}
    #     detail_page = self.detail_page()
    #     saiguo = self.parse_data_saiguo()
    #     jifen = self.parse_data_jifen()
    #     qiudui = self.parse_data_qiudui()
    #     rangqiu = self.parse_data_rangqiu()
    #     jinqiushu = self.parse_data_jinqiushu()
    #     banquanchang = self.parse_data_banquanchang()
    #     sports_item['detail_page'] = detail_page
    #     sports_item['saiguo'] = saiguo
    #     sports_item['jifen'] = jifen
    #     sports_item['qiudui'] = qiudui
    #     sports_item['rangqiu'] = rangqiu
    #     sports_item['jinqiushu'] = jinqiushu
    #     sports_item['banquanchang'] = banquanchang
    #     self.exit_chrome()
    #     return sports_item
    
class TiSpiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        # request.dont_filter = False
            # client = pymongo.MongoClient(host='192.168.77.114',port=27017)
            # dbname = client['proxypool']
            # collection = dbname['proxies']
            # proxies = collection.find({})
            # proxy =  random.choice([proxy['proxy'] for proxy in proxies])
            # proxies = {
            #     'http': 'http://' + proxy,
            #     'https': 'http://' + proxy
            # }
            # request.meta['proxy'] = proxies
        #     url = request.url
        #     data = self.run_sports(url)
        #     request.meta['data'] = data
        #     return scrapy.http.HtmlResponse(url=request.url, encoding='utf-8',request=request)
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

    # def run_sports(self,url):
    #     self.get_data(url)
    #     sports_item = {}
    #     detail_page = self.detail_page()
    #     saiguo = self.parse_data_saiguo()
    #     jifen = self.parse_data_jifen()
    #     qiudui = self.parse_data_qiudui()
    #     rangqiu = self.parse_data_rangqiu()
    #     jinqiushu = self.parse_data_jinqiushu()
    #     banquanchang = self.parse_data_banquanchang()
    #     sports_item['detail_page'] = detail_page
    #     sports_item['saiguo'] = saiguo
    #     sports_item['jifen'] = jifen
    #     sports_item['qiudui'] = qiudui
    #     sports_item['rangqiu'] = rangqiu
    #     sports_item['jinqiushu'] = jinqiushu
    #     sports_item['banquanchang'] = banquanchang
    #     self.exit_chrome()
    #     return sports_item