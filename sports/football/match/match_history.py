import datetime

import match_all_today

class MatchHistory(match_all_today.Match):
    def mac(self):
        super().mac()
        print('美杜莎女王')
        

a = MatchHistory()
url = 'https://live.leisu.com/wanchang?date=20190406'
date_start = url.split('=')[-1]
now = datetime.datetime.now().date()
date_now = now.strftime('%Y%m%d')   
while True:
    if date_now != date_start:
        try:
            a.renewMatch(url)
            date2 = datetime.datetime.strptime(date_start, '%Y%m%d').date()
            date1 = date2 + datetime.timedelta(days=1)
            date_start = date1.strftime('%Y%m%d')
            url = 'https://live.leisu.com/wanchang?date={}'.format(date_start)    
        except Exception as e:
            print('发生错误:{}，将继续重新匹配！'.format(e))
    else:
        print('爬取结束！')
        break