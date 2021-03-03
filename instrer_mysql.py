# -*- coding:utf-8 -*-
import time
from collections import OrderedDict
from conn_mysql import ConnMysqlDB

# from 爬虫.lng_卓创 import


conn = ConnMysqlDB()


def insert_Gdq_Price(dict_lists_gd):
    """
    管道气-期货：ailng_lng_gdq_1      19/3/12更新
    管道气-国际现货：ailng_lng_gdq_2    卓创不提供更新
    管道气：ailng_lng_gdq   19/3/12更新
    管道气-工业：ailng_lng_gdqgy
    管道气-民用：ailng_lng_gdqmy
    管道气-门站：ailng_lng_gdqmz
    :param dict_lists_gd:
    :return:
    """
    if len(dict_lists_gd):
        try:
            mz_gdq = dict_lists_gd['门站']
            sql = "INSERT INTO ailng_lng_gdqmz (region,province,city,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, mz_gdq)

            gy_gdq = dict_lists_gd['工业']
            sql = "INSERT INTO ailng_lng_gdqgy (region,province,city,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, gy_gdq)

            my_gdq = dict_lists_gd['民用']
            sql = "INSERT INTO ailng_lng_gdqmy (region,province,city,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, my_gdq)
        except Exception as e:
            print(e)
    else:
        print('当前无数据')


def insert_Lng_Price(dict_lists_jj, dict_lists_gj, date):
    '''
        插入数据
        液场：ailng_lng_yc
        接收站：ailng_lng_jsz
        LNG加气站零售价格：ailng_lng_retail   19/8/19更新
        FOB：ailng_lng_fob    卓创不提供更新
        CFR：ailng_lng_cfr    卓创不提供更新
        即期赁费-长期赁费：ailng_lng_ship    卓创不提供更新
        接货价：ailng_lng_delivery_price      19/3/12更新
        地区成交价：ailng_lng_data      19/3/12更新
    :param item:
    :return:
    '''
    if len(dict_lists_jj) or len(dict_lists_gj):
        try:
            j_h_j = dict_lists_jj['接货价']
            sql = "INSERT INTO ailng_lng_delivery_price (region,province,type,price_s,price_b,unit,day) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, j_h_j)

            c_j_j = dict_lists_jj['成交价']
            sql = "INSERT INTO ailng_lng_data (region,price_l,price_m,unit,day) VALUES (%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, c_j_j)

            l_s_j = dict_lists_jj['零售价']
            sql = "INSERT INTO ailng_lng_retail (region,province,city,name,type,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, l_s_j)

            c_b_j = dict_lists_gj['厂报价']
            sql = "INSERT INTO ailng_lng_yc (region,province,company,type,qihualv,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, c_b_j)

            z_b_j = dict_lists_gj['站报价']
            sql = "INSERT INTO ailng_lng_jsz (region,province,company,price,unit,day) VALUES (%s,%s,%s,%s,%s,%s)"
            conn.updateDataBatch(sql, z_b_j)
            # 处理当天的价格
            mycounter(date)
        except Exception as e:
            print(e)
    else:
        print('当前无数据')


def mycounter(date):
    '''
    处理当天价格数据，计算涨幅，排名
    ailng_rt_price
    :return:
    '''
    timeArray = time.strptime(date, "%Y-%m-%d")
    stayTime = int(time.mktime(timeArray))
    flag = '2018-09-16'
    timeArray = time.strptime(flag, "%Y-%m-%d")
    flagTime = int(time.mktime(timeArray))
    while stayTime > flagTime:
        # 当天的
        sql = "SELECT * FROM `ailng_lng_delivery_price` WHERE `day` = '%s'" % date
        res = conn.querySomeData(sql)
        if res:
            sql1 = "SELECT * FROM `ailng_rt_price` WHERE `datetime` = '%s' LIMIT 1" % date
            res1 = conn.querySomeData(sql1)
            if res1:
                break
            else:
                stayTime = stayTime - (3600 * 24)
                time_local = time.localtime(stayTime)
                date = time.strftime("%Y-%m-%d", time_local)
                # 前一天的
                sql2 = "SELECT * FROM `ailng_lng_delivery_price` WHERE `day` = '%s'" % date
                res2 = conn.querySomeData(sql2)
                insert = counter(res, res2)
                ilen = len(insert)
                for ins in insert:
                    # 0 省市 ，1时间，2平均价，3价格排名，4幅度 ，ilen幅度排名
                    sql = "INSERT INTO `ailng_rt_price` (`province`,`price`,`price_ranking`,`datetime`,`increase`,`rise_in_ranking`) " \
                          "VALUES ('%s','%s','%s','%s','%s','%s')" % \
                          (ins[0], ins[2], ins[3], ins[1], ins[4], ilen)
                    ilen = ilen - 1
                    conn.updateData(sql)
        else:
            stayTime = stayTime - (3600 * 24)
            time_local = time.localtime(stayTime)
            date = time.strftime("%Y-%m-%d", time_local)

    # 更新ADcode
    sql = 'UPDATE ailng_rt_price SET ADcode = (SELECT ailng_rt_adcode.areacode FROM ailng_rt_adcode WHERE ailng_rt_price.province = ailng_rt_adcode.areaname) WHERE ADcode is NULL;'
    conn.updateData(sql)


def counter(c1, c2):
    '''
    处理价格涨幅，排名，广东省单独处理
    :param c1: 今天的接货价
    :param c2: 前一天的接货价
    :return:
    '''
    try:
        d1 = OrderedDict()
        d2 = OrderedDict()
        l1 = []
        count = 0
        price_sum = 0
        for v1 in c1:
            agv = int(v1[5] + v1[6]) / 2
            if v1[2] in ['广东珠海', '广东广州', '广东潮汕']:
                count += 1
                price_sum += agv
                if count == 3:
                    d1['广东省'] = [v1[8], int(price_sum / 3)]
                    l1.append(int(price_sum / 3))

                continue

            if v1[2] == '市场名称':
                continue

            d1[v1[2]] = [v1[8], agv]
            l1.append(agv)

        l2 = []
        count = 0
        price_sum = 0
        for v2 in c2:
            agv = int(v2[5] + v2[6]) / 2
            if v2[2] in ['广东珠海', '广东广州', '广东潮汕']:
                count += 1
                price_sum += agv
                if count == 3:
                    d2['广东省'] = [v2[8], price_sum / 3]
                    l2.append(int(price_sum / 3))

                continue

            d2[v2[2]] = [v2[8], agv]
            l2.append(agv)
        l1 = whateveryoucallit(l1)
        l1 = myinit(l1)
        # l2 = whateveryoucallit(l2)
        # l2 = myinit(l2)
        res = []
        for val in d1:
            r1 = d1[val]
            if val in d2:
                r2 = d2[val]
                res.append([val, r1[0], r1[1], l1[r1[1]], round(abs((r1[1] - r2[1])) / r2[1], 2)])
        res = sorted(res, key=lambda t: t[4])
        return res
    except Exception as e:
        print(e)


def myinit(l):
    ret = {}
    for v in l:
        for vv in v:
            ret[vv] = v[vv]
    return ret


def whateveryoucallit(lis):
    copylis = lis[:]
    copylis.sort(reverse=True)
    dic = {v: i + 1 for i, v in enumerate(copylis)}
    return [{v: dic[v]} for i, v in enumerate(lis)]


def select_Db_Price():
    sql = "select day from ailng_lng_delivery_price desc limit 1"
    pass


if __name__ == '__main__':
    pass
