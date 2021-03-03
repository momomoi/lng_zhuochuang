from sqlalchemy import create_engine
import pymysql

HOST = '60.205.223.143'
PORT = 3306
USER = 'root'
PASSWD = 'Zhongyi_mysql_60'
DB = 'cnooc_map'


class ConnMysqlDB(object):
    """
    连接与操作数据库
    """

    def __init__(self, host=HOST, port=PORT, user=USER, passwd=PASSWD, db=DB):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.conn = None  # 数据库连接对象
        self.cursor = None  # 操作数据库的游标
        self.connMysql()

    # 连接数据库
    def connMysql(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.passwd,
                                    database=self.db)
        self.cursor = self.conn.cursor()
        return self.conn

    def closeConn(self):
        self.cursor.close()
        self.conn.close()

    def coonEngine(self):
        """
        连接数据库，用于pandas
        pandas 的数据库储存方式特殊，需要用这个函数连接才能操作
        :return:
        """
        sql = 'mysql+pymysql://' + self.user + ':' + self.passwd + '@' + self.host + ':' + str(
            self.port) + '/' + self.db + '?charset=utf8'
        conn = create_engine(sql)
        return conn

    # 查询一条记录
    def queryOneData(self, sql=''):
        self.cursor.execute(sql)
        self.conn.commit()
        st = self.cursor.fetchone()
        return st

        # 查询一条记录

    def querySomeData(self, sql=''):
        self.cursor.execute(sql)
        self.conn.commit()
        st = self.cursor.fetchall()
        return st

    # 更新单条记录
    def updateData(self, sql=''):
        self.cursor.execute(sql)
        self.conn.commit()
        return True

    def updateDataBatch(self, sql='', data=()):
        """
        批量更新数据，data为tuple
        """
        self.cursor.executemany(sql, data)
        self.conn.commit()
        return True

    def delect_data(self, table_name):
        """
        清除指定表中所有数据，保留格式
        :param table_name: 需清除的表名
        :return:
        """
        sql = "truncate  %s" % table_name
        self.updateData(sql)
