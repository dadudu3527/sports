import logging
logger = logging.getLogger('yuezi.log')
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()#控制台输出
sh.setLevel(logging.DEBUG)

fh = logging.FileHandler('E://学习ppt/mysql/xiayin.txt')#写入文件
fh.setLevel(logging.WARNING)

formatter = logging.Formatter('%(name)s-%(levelname)s-%(message)s-%(asctime)s')
sh.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(sh)
logger.addHandler(fh)