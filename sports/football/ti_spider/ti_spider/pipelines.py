# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import pymysql
import json
from .items import *

# class TiSpiderPipeline(object):
#
#     def open_spider(self,spider):
#         self.f = open('E://downloader//tiyu.txt','w')
#     def process_item(self, item, spider):
#         if isinstance(item, TiSpiderItem):
#             self.f.write(json.dumps(dict(item),ensure_ascii=False)+'\n')
#         return item
#     def close_spider(self,spider):
#         self.f.close()
#
# class LanSpiderPipeline(object):
#     def open_spider(self,spider):
#         self.f = open('E://downloader//lanqiu.txt','w')
#     def process_item(self, item, spider):
#         if isinstance(item, LanqiuSpiderItem):
#             self.f.write(json.dumps(dict(item),ensure_ascii=False)+'\n')
#         return item
#     def close_spider(self,spider):
#         self.f.close()
#
# class ZhuSpiderPipeline(object):
#     def open_spider(self,spider):
#         self.f = open('E://downloader//url.txt','w')
#     def process_item(self, item, spider):
#         # if isinstance(item, LanqiuSpiderItem):
#         self.f.write(json.dumps(dict(item),ensure_ascii=False)+'\n')
#         return item
#     def close_spider(self,spider):
#         self.f.close()
#
# class ZhuQiuSpiderPipeline(object):
#     config = {
#             'host': '192.168.77.114',
#             'port': 27017,
#         }
#     def open_spider(self,spider):
#         client = pymongo.MongoClient(**self.config)
#         db = client['football']
#         self.collection = db['season']
#     def process_item(self, item, spider):
#         if isinstance(item,ZhuqiuSpiderItem):
#             col = self.collection.find_one(dict(item))
#             if not col:
#                 self.collection.insert_one(dict(item))
#         return item
#     def close_spider(self,spider):
#         pass
# class RuleSpiderPipeline(object):
#     def open_spider(self,spider):
#         client = pymongo.MongoClient()
#         db = client['football']
#         self.collection = db['season']
#     def process_item(self, item, spider):
#         if isinstance(item,RuleSpiderItem):
#             self.collection.update_one({'leagueId':dict(item).get('leagueId')},{'$set':dict(item)})
#         return item
#     def close_spider(self,spider):
#         pass
class ZhuqiuSpiderPipeline(object):
    def open_spider(self,spider):
        client = pymongo.MongoClient('mongodb://root:DRsXT5ZJ6Oi55LPQ@dds-wz90ee1a34f641e41.mongodb.rds.aliyuncs.com:3717,dds-wz90ee1a34f641e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-15344719')
        db = client['football']
        self.collection = db['season']
    def process_item(self, item, spider):
        if isinstance(item,ZhuqiuSpiderItem):
            note = self.collection.find_one({'seasonId':dict(item).get('seasonId')})
            if not note:
                self.collection.insert_one(dict(item))
            # self.collection.update_one({'seasonId':dict(item).get('seasonId')},{'$set':dict(item)})
        return item
    def close_spider(self,spider):
        pass
#
# class LanqiuPipeline(object):
#     config = {
#         'user':'root',
#         'password':'qwe123',
#         'db':'feiluo',
#         'charset':'utf8'
#     }
#     def open_spider(self,spider):
#         self.conn = pymysql.connect(**self.config)
#         self.cur = self.conn.cursor()
#     def process_item(self,item,spider):
#         if isinstance(item,LanqiuSpiderItem):
#             try :
#                 self.cur.execute('insert into ma(yaoqiu) values("%s")'%dict(item)['yaoqiu'])
#             except Exception as e:
#                 print('写入数据库失败！错误原因%s'%e)
#                 self.conn.rollback()
#             else:
#                 self.conn.commit()
#     def close_spider(self,spider):
#         self.cur.close()
#         self.conn.close()


