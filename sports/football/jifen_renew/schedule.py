from multiprocessing import Process
from .jifen1 import TiyuData
from .jifen2 import TiyuData
from .jifen3 import TiyuData
from .jifen4 import TiyuData
from .jifen5 import TiyuData
from .jifen6 import TiyuData
from .jifen7 import TiyuData
from .jifen8 import TiyuData
from .jifen9 import TiyuData
from .jifen10 import TiyuData
from .jifen11 import TiyuData
from .jifen12 import TiyuData
from .jifen13 import TiyuData
from .jifen14 import TiyuData
from .jifen15 import TiyuData
from .jifen16 import TiyuData
from .jifen17 import TiyuData 
from .jifen18 import TiyuData 
from .jifen19 import TiyuData 
from .jifen20 import TiyuData 


from .jifen1 import crawler as crawler1
from .jifen2 import crawler as crawler2
from .jifen3 import crawler as crawler3
from .jifen4 import crawler as crawler4
from .jifen5 import crawler as crawler5
from .jifen6 import crawler as crawler6
from .jifen7 import crawler as crawler7
from .jifen8 import crawler as crawler8
from .jifen9 import crawler as crawler9
from .jifen10 import crawler as crawler10
from .jifen11 import crawler as crawler11
from .jifen12 import crawler as crawler12
from .jifen13 import crawler as crawler13
from .jifen14 import crawler as crawler14
from .jifen15 import crawler as crawler15
from .jifen16 import crawler as crawler16
from .jifen17 import crawler as crawler17
from .jifen18 import crawler as crawler18
from .jifen19 import crawler as crawler19
from .jifen20 import crawler as crawler20
from queue import Queue
import pymongo 

from .config import config

item = {1:crawler1,2:crawler2,3:crawler3,4:crawler4,5:crawler5,6:crawler6,7:crawler7,
            8:crawler8,9:crawler9,10:crawler10,11:crawler11,12:crawler12,13:crawler13,
            14:crawler14,15:crawler15,16:crawler16,17:crawler17,18:crawler18,19:crawler19,20:crawler20}

queue = Queue()
client = pymongo.MongoClient(config)
dbname = client['football']
collection = dbname['season']
data = collection.find()
for i in data:
    id = i.get('seasonId')
    queue.put(id)

def func(crawler):
    a = crawler()
    while True:
        id = queue.get()
        url = 'https://data.leisu.com/zuqiu-{}'.format(id)
        try:
            # pool.apply_async(self.parse_data, args=(url,))
            a.parse_data(url)
        except Exception as e:
            errorId = self.collection_error.find_one({'seasonId':i})
            if not errorId:
                self.collection_error.insert_one({'seasonId':i})
            logger.info('{}下载失败！{}'.format(e, i))
        queue.task_done()
        
           
def run_jifen():
    for i in range(1,21):
        p = Process(target=func,args=(item.get(i),))
        p.start()
        
   
    

    
