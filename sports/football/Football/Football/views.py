import redis
from django.http import HttpResponse
from django.shortcuts import redirect,reverse,render
from .team import *
import json

def team(request):
    teamId = request.GET.get('teamid')
    if not teamId:
        return HttpResponse('参数出错！')
    else:
        res = Team().crm(int(teamId))
        return  HttpResponse(json.dumps(res,ensure_ascii=False))

def player(request):
    playerId = request.GET.get('playerid')
    teamId = request.GET.get('teamid','')
    if not playerId:
        return HttpResponse('参数出错！')
    else:
        res = Player().run_player(playerId,teamId)
        return HttpResponse(json.dumps(res, ensure_ascii=False))
        
def season(request):
    mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')
    seasonId = request.GET.get('seasonId')
    if not seasonId:
        return HttpResponse('参数出错！')
    note = mdb.get(seasonId)
    if note:
        return HttpResponse('请求频率要低于2小时/次')
    else:
        if int(seasonId) != 8796 and int(seasonId) != 8457:
            mdb.sadd('getSeasonUrl',seasonId)
            mdb.sadd('SeasonMatch',seasonId)
            mdb.set(seasonId,seasonId)
            mdb.expire(seasonId,60*60*2)
            return HttpResponse('seasonId加入队列成功！')