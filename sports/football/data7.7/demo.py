import requests

# 要访问的目标页面
targetUrl = "http://api.leisu.com/api/odds/detail?sid=2506352"

# 代理服务器
proxyHost = "http-pro.abuyun.com"
proxyPort = "9010"

# 代理隧道验证信息
proxyUser = "HGT5988523KT96CP"
proxyPass = "2787C99DA8A3B941"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {"host": proxyHost, "port": proxyPort, "user": proxyUser,
    "pass": proxyPass, }

proxies = {"http": proxyMeta, "https": proxyMeta, }

resp = requests.get(targetUrl, proxies=proxies)
