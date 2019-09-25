# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 10:29
# @Author   : 马鹏
# @Software : PyCharm
import requests
import logging
import chardet
import time
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
class DownLoader:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        }
    def download(self,url,ruler=None):
        logging.info('Downloading:{}'.format(url))
        try:
            resp = requests.get(url,headers=self.headers)
            bianma = chardet.detect(resp.content)['encoding']
            resp.encoding = bianma
            delay = ruler.get('delay')
            if delay :
                time.sleep(delay)
            if resp.status_code == 200:
                return resp.text
            else :
                raise ConnectionError
        except ConnectionError as e:
            logging.ERROR(e)

if __name__ == '__main__':
    d = DownLoader()
    d.download('www')