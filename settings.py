import logging

# mysql设置
HOST = '***'
PORT = 3306
USER = 'root'
PASSWD = '***'
DB = '***'
CHATSET = 'utf8'

# log 级别
LOG_LEVEL = logging.ERROR  # DEBUG,INFO,WARNING,ERROR,CRITICAL
# log 储存位置
LOG_FILE = './error.log'
# log 格式
LOG_FORMAT = '[%(asctime)s][%(levelname)s]%(message)s'
# log 全局设置
logging.basicConfig(level=LOG_LEVEL,
                    format=LOG_FORMAT,
                    filename=LOG_FILE,
                    filemode='a')
