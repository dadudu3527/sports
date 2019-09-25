# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 11:09
# @Author   : 马鹏
# @Software : PyCharm

from .config import parse_ruler
from .downloader import DownLoader
from .myparse import Parser
from .validate import valid_many
from ..db import MongoDB
import logging
import time

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

def crawler():
    while True:
        logger.info('开始抓取可用代理')
        for ruler in parse_ruler:
            for url in ruler['url']:
                text = DownLoader().download(url,ruler)
                proxy_list = Parser().parse(text,ruler)
                valid_many(proxy_list,'crawler')
        time.sleep(10)
def check():
    while True:
        m = MongoDB()
        proxies = m.my_find(100000)
        if not len(proxies) == 0:
            logger.info('开始检测数据库代理可用性')
            valid_many(proxies,'check')
        time.sleep(5)
if __name__ == '__main__':
    crawler()