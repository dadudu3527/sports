# -*- coding: utf-8 -*-
# @Time     : 2019/6/9 10:56
# @Author   : 马鹏
# @Software : PyCharm

parse_ruler = [{
                'name':'xici',
                'url':['https://www.xicidaili.com/nn/{}'.format(i) for i in range(1,4)],
                'parse_type':'xpath',
                'ip':'//*[@id="ip_list"]/tr/td[2]/text()',
                'port':'//*[@id="ip_list"]/tr/td[3]/text()',
                },
                {
                    'name':'kuaidaili',
                    'url':['https://www.kuaidaili.com/free/inha/{}/'.format(i) for i in range(1,7)],
                    'parse_type':'xpath',
                    'ip':'//tbody/tr/td[1]/text()',
                    'port':'//tbody/tr/td[2]/text()',
                    'delay': 1 ,
                },
                {
                    'name':'kuaidaili2',
                    'url':['https://www.kuaidaili.com/ops/proxylist/{}/'.format(i) for i in range(1,7)],
                    'parse_type':'re',
                    'ip':r'<td data-title="IP">(.*?)</td>',
                    'port':r'<td data-title="PORT">(.*?)</td>',
                },
               ]