# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 15:38
# @Author   : 马鹏
# @Software : PyCharm

import pymongo
import random

class MongoDB:
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client['proxypool']
        self.collection = self.db['proxies']
        self.collection.ensure_index('proxy',unique=True) #数据库去重
    def my_insert(self,value,one=True):
        if one :
            self.collection.insert_one(value)
        else:
            self.collection.insert_many(value)
    def my_remove(self,value={}):
        self.collection.remove(value)
    def my_delete(self,value,one=True):
        if one:
            self.collection.delete_one(value)
        else:
            self.collection.delete_many(value)
    def my_update(self,one=True,*value):
        if one :
            self.collection.update_one(*value)
        else :
            self.collection.update_many(*value)
    def my_find(self,count,value={},one=False):
        if one :
            result=self.collection.find_one(value)

        else :
            res=self.collection.find(value,limit=count)
            result = [i.get('proxy') for i in res]
        return result

    def get_random_proxy(self):
        proxies = self.collection.find({})
        return random.choice([proxy['proxy'] for proxy in proxies])

    def get_fast_proxy(self):
        proxies = self.collection.find({}).sort('delay')
        return [proxy['proxy'] for proxy in proxies][0]#提取最快的
if __name__ == '__main__':
    m = MongoDB()
    m.my_insert({'proxy': '180.119.141.108:9999', 'delay': 0.5610320568084717})
