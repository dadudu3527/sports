# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 18:46
# @Author   : 马鹏
# @Software : PyCharm
import flask
from .db import MongoDB
import json
app = flask.Flask(__name__)

@app.route('/getFastProxy')
def get_Fast_proxy():
    return MongoDB().get_fast_proxy()

@app.route('/getRandomProxy')
def get_Random_proxy():
    return MongoDB().get_random_proxy()

@app.route('/getProxies')
def get_proxies():
    args = dict(flask.request.args)
    return json.dumps(MongoDB().my_find(int(args['count'][0])))

@app.route('/deleteData')
def m_delete():
    args = dict(flask.request.args)
    try:
        MongoDB().my_delete({'proxy':args['proxy'][0]})
        return '删除成功'
    except Exception:
        return '删除失败'
def api_run():
    app.run(port='12321')
