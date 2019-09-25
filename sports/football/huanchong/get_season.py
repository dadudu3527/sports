import redis
import time
from ..match.match_rw import Match
ma = Match()
tup = ma.current_season()
print(tup)
# mdb = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True,password='fanfubao')

        
# mdb.sadd('seasonUrl',*liu)


# le = mdb.llen('list_seasonUrl')

# print(le)
# while True:
    # print('开始从redis读取。。。')
    # note = mdb.lpop('list_seasonUrl')
    # if note:
        # print(note)
    # else:
        # print('开始睡眠...')
        # time.sleep(60*10)