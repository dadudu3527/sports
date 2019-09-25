# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 14:46
# @Author   : 马鹏
# @Software : PyCharm

import time
import requests
from ..db import MongoDB
from multiprocessing.pool import ThreadPool
from requests.exceptions import ProxyError,ConnectionError,ConnectTimeout,HTTPError
import logging

logger = logging.getLogger(__name__)
sh =logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

def valid_many(proxy_list,method):
    pool = ThreadPool(30)
    for proxy in proxy_list:
        pool.apply_async(valid,args=(proxy,method))
    pool.close()
    pool.join()

def valid(proxy,method,url='https://data.leisu.com/'):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
    }
    url = url
    proxies = {
        'http':'http://'+ proxy['proxy'],
        'https':'http://'+proxy['proxy']
    }
    try:
        start = time.time()
        resp = requests.get(url=url,headers=headers,proxies=proxies,timeout=5)
        delay = round(time.time() - start,2)
        if resp.status_code == 200:
            if method == 'crawler':
                proxy['delay'] = delay
                MongoDB().my_insert(proxy)
            elif method == 'check':
                MongoDB().my_update({'proxy':proxy['proxy']},{'$set':{'delay':delay}})
            logger.info('此代理可用{}'.format(proxy))
        else:
            if method == 'check':
                MongoDB().my_delete({'proxy':proxy['proxy']})
                logger.info('此代理失效{}'.format(proxy))
    except (ProxyError,ConnectionError,ConnectTimeout,HTTPError):
        if method == 'check':
            MongoDB().my_delete({'proxy': proxy['proxy']})
            logger.info('此代理失效{}'.format(proxy))