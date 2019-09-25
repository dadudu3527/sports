# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 10:49
# @Author   : 马鹏
# @Software : PyCharm
from lxml import etree
import re

class Parser:
    def get_proxy_list(self,ip_port_list):
        proxy_list = []
        for ip,port in ip_port_list:
            proxy = {
                'proxy':ip+':'+port
            }
            proxy_list.append(proxy)
        return proxy_list
    def xpath_parse(self,text,ruler):
        page = etree.HTML(text)
        ip_list =  page.xpath(ruler['ip'])
        port_list =  page.xpath(ruler['port'])
        return zip(ip_list, port_list)
    def re_parse(self,text,ruler):
        ip_list = re.findall(ruler['ip'],text)
        port_list = re.findall(ruler['port'],text)
        return zip(ip_list, port_list)
    def parse(self,text,ruler):
        if ruler['parse_type'] == 're':
            ip_port_list = self.re_parse(text,ruler)
        elif ruler['parse_type'] == 'xpath':
            ip_port_list = self.xpath_parse(text,ruler)
        else:
            raise ValueError('parse ruler 没有指定解析类型')
        proxy_list = self.get_proxy_list(ip_port_list)
        return proxy_list
