# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TiSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    match_name = scrapy.Field() #赛事
    icon_url = scrapy.Field()  # 图标网址
    rotary = scrapy.Field() # 轮次
    match_time  = scrapy.Field() #比赛时间
    match_status = scrapy.Field() #状态
    home_team = scrapy.Field() # 主场球队
    finish_score = scrapy.Field() #完场比分
    away_team = scrapy.Field() #客场球队
    half_score = scrapy.Field()  #半场比分
    corner = scrapy.Field() #角球
    success_or_failure = scrapy.Field() #胜负
    concede_points = scrapy.Field() #让球
    goals_for = scrapy.Field() # 进球数
    match_datetime = scrapy.Field()

class ZhuqiuSpiderItem(scrapy.Item):
    # detail_title = scrapy.Field()
    # target_url = scrapy.Field()
    # detail_page = scrapy.Field()
    # saiguo = scrapy.Field()
    # jifen = scrapy.Field()
    # qiudui = scrapy.Field()
    # rangqiu = scrapy.Field()
    # jinqiushu = scrapy.Field()
    # banquanchang = scrapy.Field()
    leagueId = scrapy.Field()
    seasonId = scrapy.Field()
    seasonYear = scrapy.Field()
    area = scrapy.Field()
    country = scrapy.Field()
    leagueName = scrapy.Field()
    leagueFullName = scrapy.Field()

class LanqiuSpiderItem(scrapy.Item):
    match_name = scrapy.Field()
    data = scrapy.Field()
    match_status = scrapy.Field()
    match_time = scrapy.Field()
    match_datetime = scrapy.Field()

class RuleSpiderItem(scrapy.Item):
    leagueId = scrapy.Field()
    season = scrapy.Field()
    matchRule = scrapy.Field()

