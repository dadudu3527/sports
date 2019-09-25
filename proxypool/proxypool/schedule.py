# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 20:55
# @Author   : 马鹏
# @Software : PyCharm

from multiprocessing import Process
from .api import api_run
from .spider.crawler import crawler,check

def run_proxypool():
    api_process = Process(target=api_run)
    crawler_process = Process(target=crawler)
    check_process = Process(target=check)

    api_process.start()
    crawler_process.start()
    check_process.start()

    api_process.join()
    crawler_process.join()
    check_process.join()
