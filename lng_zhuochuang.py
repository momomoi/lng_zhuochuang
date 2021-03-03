import datetime
import logging
import requests
import warnings
import json
from chinese_calendar import is_workday
from instrer_mysql import insert_Lng_Price, insert_Gdq_Price, select_Db_Price

warnings.filterwarnings('ignore')

# from fake_useragent import UserAgent
#
# U = UserAgent()
# ua = U.random
#
# headers = {
#     "Accept": "*/*",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Accept-Language": "zh-CN,zh;q=0.9",
#     "Connection": "keep-alive",
#     "Content-Length": '265',
#     "Content-Type": "application/json",
#     "User-Agent": ua,
#     "X-Requested-With": "XMLHttpRequest",
#     # "Referer": "http://price.sci99.com/view/priceview.aspx?pagename=energyview&classid=1432&pricetypeid=25&pricecondition=%e5%9c%b0%e5%8c%ba%e6%88%90%e4%ba%a4%e4%bb%b7&linkname=lng%e5%9c%b0%e5%8c%ba%e6%88%90%e4%ba%a4%e4%bb%b7&Token=c20cf19398cbcbe6&RequestId=895a2ddae11e29a9",
#     "Host": "prices.sci99.com",
#     "Origin": "https://prices.sci99.com",
#     "Sec-Fetch-Dest": "empty",
#     "Sec-Fetch-Mode": "cors",
#     "Sec-Fetch-Site": "same-origin"}

jh_q = [
    # LNG下游接货价
    {"cycletype": "day", "specialpricetype": "LNG下游接货价", "ppname": "LNG", "pricetypeid": 34320, "ppids": "12480",
     "navid": "255", "sitetype": 1, "pageno": 1, "pagesize": 300},
    # LNG地区成交价
    {"cycletype": "day", "specialpricetype": "LNG地区成交价", "ppname": "LNG", "pricetypeid": 34320, "ppids": "12480",
     "navid": "255", "sitetype": 1, "pageno": 1, "pagesize": 300},
    # LNG加气站零售价
    {"cycletype": "day", "specialpricetype": "LNG加气站零售价", "ppname": "LNG", "pricetypeid": 34320, "ppids": "12480",
     "navid": "255", "sitetype": 1, "pageno": 1, "pagesize": 300}
]

gc_jsz = [
    # 工厂价
    {"cycletype": "day", "specialpricetype": "LNG工厂价", "ppname": "LNG", "province": "", "pricetypeid": 34319,
     "ppids": "12480", "navid": "255", "sitetype": 1, "pageno": 1, "pagesize": 300},
    # 接收站
    {"cycletype": "day", "specialpricetype": "LNG接收站价", "ppname": "LNG", "pricetypeid": 34319, "ppids": "12480",
     "navid": "255", "sitetype": 1, "pageno": 1, "pagesize": 300}
]

gdq_jg = [
    # 市场价格
    {"cycletype": "day", "ppname": "管道气", "pricetypeid": 34320, "ppids": "12481", "navid": "257", "sitetype": 1,
     "pageno": 1, "pagesize": 300},
]

gdq_gw = [
    # #国际价格
    {"cycletype": "day", "ppname": "管道气", "pricetypeid": 34318, "ppids": "12481", "navid": "257", "sitetype": 1,
     "pageno": 1, "pagesize": 300},
    # #期货价格
    {"cycletype": "day", "ppname": "管道气", "pricetypeid": 34327, "ppids": "12481", "navid": "257", "sitetype": 1,
     "pageno": 1, "pagesize": 300}
]


class zhuochuang(object):
    def __init__(self):
        self.headers = headers
        self.Cookie = ''
        self.url = 'https://prices.sci99.com/api/zh-cn/product/datavalue'
        self.dict_lists_jj = {}
        self.dict_lists_gj = {}
        self.dict_lists_gd = {}
        self.time_local = datetime.date.today()

    def run(self):
        # cookies处理，待完善
        self.cookies()
        # 获取当前数据库存储的最新日期数据,待完善
        select_Db_Price()

        # 时间处理
        time_local = datetime.date.today() + datetime.timedelta(days=-0)
        dt_1 = time_local.strftime("%Y-%m-%d")  # 时间格式转换
        dt_2 = time_local.strftime("%Y/%m/%d")  # 时间格式转换

        # dt_1 = self.time_local.strftime("%Y-%m-%d")  # 时间格式转换
        # dt_2 = self.time_local.strftime("%Y/%m/%d")  # 时间格式转换
        # 处理节假日问题
        if is_workday(self.time_local):
            self.jh_q(dt_1, dt_2)  # 获取LNG下游接货价、LNG地区成交价、LNG地区零售价
            self.gc_jsz(dt_1, dt_2)  # 获取液场和接收站到价格
            self.gdq_jg(dt_1, dt_2)  # 获取管道气相关相关价格信息
            # self.gdq_gw(dt_1,dt_2)   #获取管道气国际和期货价格
            print(self.dict_lists_jj)
            print(self.dict_lists_gj)
            print(self.dict_lists_gd)
            insert_Lng_Price(self.dict_lists_jj, self.dict_lists_gj, dt_1)  # lng信息存入数据库
            insert_Gdq_Price(self.dict_lists_gd)  # 管道气信息存入数据库

        else:
            print('节假日')

    def cookies(self):
        'PowerStatus:1   正常'
        'PowerStatus:2   无权限'
        'PowerStatus:3   未登录'
        # Cookie = 'navSec=17; navfirst=4; UM_distinctid=17683c75e5c338-096398da604b62-6d112d7c-1aeaa0-17683c75e5e8d1; guid=87d89d92-f1ee-a28e-c808-6485478cef5e; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1608528650; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=zuh13g1rgbjb4bzayh2tpm15; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1608531258; STATReferrerIndexId=1; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E5%85%B6%E4%BB%96%E7%BD%91%E7%AB%99; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E6%9C%AA%E7%9F%A5; Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608620100,1608620113,1608620121,1608620428; STATcUrl=; pageViewNum=48;Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=%s'
        # Cookie = 'guid=12edb48d-decb-6105-7e86-d8cbeb1582d3; STATReferrerIndexId=1; route=258ceb4bb660681c2cb2768af9756936; ASP.NET_SessionId=4l3ep3obvorx2xpzesnhks3i; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E5%85%B6%E4%BB%96%E7%BD%91%E7%AB%99; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E6%9C%AA%E7%9F%A5; Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608696958,1608696968,1608696974; pageViewNum=4; Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608696990'
        # Cookie = 'guid=e5ea0bd1-5575-bba0-eb90-b50de21b4ac9; STATReferrerIndexId=1; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=d2qfxyyzjck24erfsuebp1c4; href=https%3A%2F%2Fprices.sci99.com%2Fcn%2F; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E5%85%B6%E4%BB%96%E7%BD%91%E7%AB%99; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E6%9C%AA%E7%9F%A5; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1605585530,1605592566,1605593164,1608104752; pageViewNum=4; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1608104752; openChatb101a8c0-85cc-11ea-b67c-831fe7f7f53e=true;  Hm_lvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608106071,1608106079,1608106084,1608771985;  Hm_lpvt_78a951b1e2ee23efdc6af2ce70d6b9be=1608772000'
        # Cookie = "ASP.NET_SessionId=3fw35w2vaag2qza5vkzyp0ld"
        Cookie = "ASP.NET_SessionId=f2z2jiw3ferxto0a0ni2dynt"

        c_arr = Cookie.split(';')
        c_dict = {}
        for c in c_arr:
            arr = c.split('=')
            c_dict[arr[0]] = arr[1]
        self.Cookie = c_dict

    def jh_q(self, dt_1, dt_2):
        try:
            for parameter_list in jh_q:
                single_lists = []
                split_d = ''
                ret = requests.post(self.url, data=json.dumps(parameter_list), headers=headers, cookies=self.Cookie)
                data_json = (ret.text)
                datas = json.loads(data_json)
                if datas['data']['data']['Items'][0]['PowerStatus'] != 1:
                    print('当前无权限，停止获取,当前时间为%s' % (datetime.date.today()))
                    break
                data = datas['data']['data']['Items']
                for item in data:
                    split_d = item['DataName'][-3:]
                    if split_d == '接货价':
                        if item['DataModelName'] == '双数据':
                            if item[dt_2] != '-':
                                Jg_day = item[dt_2].split('-')  # 当天价格
                                Jg_s = Jg_day[0]  # 当天最低价
                                Jg_b = Jg_day[1]  # 当天最高价
                            else:
                                Jg_s, Jg_b = '', ''
                            Region = item['Region'] + '地区'  # 区域
                            Province = item['Province']  # 省份
                            if Province == '广东省':
                                Province = Province[0:2] + item['MarketSampleName']
                            elif len(Province) > 3:
                                Province = self.Province_cv(Province)
                            Unit = item['Unit']  # 单位
                            single_list = (Region, Province, split_d, Jg_s, Jg_b, Unit, dt_1)
                            single_lists.append(single_list)

                    if split_d == '成交价':
                        if item['DataModelName'] == '双数据':
                            if item[dt_2] != '-':
                                Jg_day = item[dt_2].split('-')  # 当天价格
                                Jg_s = Jg_day[0]  # 当天最低价
                                Jg_b = Jg_day[1]  # 当天最高价
                            else:
                                Jg_s, Jg_b = '', ''
                            Province = item['Province']  # 省
                            if len(Province) > 3:
                                Province = self.Province_cv(Province)
                            Unit = item['Unit']  # 单位
                            single_list = (Province, Jg_s, Jg_b, Unit, dt_1)
                            single_lists.append(single_list)

                    if split_d == '零售价':
                        if item[dt_2] != '-':
                            Jg_day = item[dt_2]  # 当天价格
                        else:
                            Jg_day = ''
                        Region = item['Region'] + '地区'  # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        Area = item['Area']  # 市
                        Model = item['Model']
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, Area, 'LNG', Model, Jg_day, Unit, dt_1)
                        single_lists.append(single_list)
                self.dict_lists_jj[split_d] = single_lists
        except Exception as e:
            logging.info(e)

    # 获取液场和接收站到价格
    def gc_jsz(self, dt_1, dt_2):
        try:
            for parameter_list in gc_jsz:
                single_lists = []
                split_d = ''
                ret = requests.post(self.url, data=json.dumps(parameter_list), headers=headers, cookies=self.Cookie)
                data_json = (ret.text)
                datas = json.loads(data_json)
                if datas['data']['data']['Items'][0]['PowerStatus'] != 1:
                    print('当前无权限，停止获取,当前时间为%s' % (datetime.date.today()))
                    break
                data = datas['data']['data']['Items']
                for item in data:
                    split_d = item['DataName'][-3:]
                    if split_d == '厂报价':
                        Jg_day = item[dt_2]  # 当天价格
                        if Jg_day == '-':
                            Jg_day = ''
                        elif item['DataModelName'] == '双数据':
                            Jg_day = item[dt_2].split('-')  # 当天价格
                            Jg_day = int((int(Jg_day[0]) + int(Jg_day[1])) / 2)  # 当天均价
                        Region = item['Region'] + '地区'  # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        Fac_Name = item['FactorySampleName']  # 液场名称
                        Model = item['Model']  # 类型
                        GasRate = item['GasRate']  # 气化率
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, Fac_Name, Model, GasRate, Jg_day, Unit, dt_1)
                        single_lists.append(single_list)

                    if split_d == '站报价':
                        Jg_day = item[dt_2]  # 当天价格
                        if Jg_day == '-':
                            Jg_day = ''
                        elif item['DataModelName'] == '双数据':
                            Jg_day = item[dt_2].split('-')  # 当天价格
                            Jg_day = int((int(Jg_day[0]) + int(Jg_day[1])) / 2)  # 当天均价
                        Region = item['Region'] + '地区'  # # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        Fac_Name = item['FactorySampleName']  # 接收站名称
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, Fac_Name, Jg_day, Unit, dt_1)
                        single_lists.append(single_list)
                self.dict_lists_gj[split_d] = single_lists
        except Exception as e:
            logging.info(e)

    def gdq_jg(self, dt_1, dt_2):
        try:
            for parameter_list in gdq_jg:
                single_lists_mz = []
                single_lists_gy = []
                single_lists_my = []
                split_d = ''
                ret = requests.post(self.url, data=json.dumps(parameter_list), headers=headers, cookies=self.Cookie)
                data_json = (ret.text)
                datas = json.loads(data_json)
                if datas['data']['data']['Items'][0]['PowerStatus'] != 1:
                    print('当前无权限，停止获取,当前时间为%s' % (datetime.date.today()))
                    break
                data = datas['data']['data']['Items']
                for item in data:
                    split_d = item['Model']
                    if split_d == '门站价':
                        Jg_day = item[dt_2]  # 当天价格
                        if Jg_day == '-':
                            Jg_day = ''
                        Region = item['Region'] + '地区'  # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        MarketSampleName = item['MarketSampleName']  # 市
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, MarketSampleName, Jg_day, Unit, dt_1)
                        single_lists_mz.append(single_list)

                    if split_d == '工业':
                        Jg_day = item[dt_2]  # 当天价格
                        if Jg_day == '-':
                            Jg_day = ''
                        Region = item['Region'] + '地区'  # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        MarketSampleName = item['MarketSampleName']  # 市
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, MarketSampleName, Jg_day, Unit, dt_1)
                        single_lists_gy.append(single_list)

                    if split_d == '民用':
                        Jg_day = item[dt_2]  # 当天价格
                        if Jg_day == '-':
                            Jg_day = ''
                        Region = item['Region'] + '地区'  # 区域
                        Province = item['Province']  # 省份
                        if len(Province) > 3:
                            Province = self.Province_cv(Province)
                        MarketSampleName = item['MarketSampleName']  # 市
                        Unit = item['Unit']  # 单位
                        single_list = (Region, Province, MarketSampleName, Jg_day, Unit, dt_1)
                        single_lists_my.append(single_list)
                self.dict_lists_gd['门站'] = single_lists_mz
                self.dict_lists_gd['工业'] = single_lists_gy
                self.dict_lists_gd['民用'] = single_lists_my
        except Exception as e:
            logging.info(e)

    def gdq_gw(self, dt_1, dt_2):
        try:
            for parameter_list in gdq_gw:
                single_lists = []
                ret = requests.post(self.url, data=json.dumps(parameter_list), headers=headers, cookies=self.Cookie)
                data_json = (ret.text)
                datas = json.loads(data_json)
                if datas['data']['data']['Items'][0]['PowerStatus'] != 1:
                    print('当前无权限，停止获取,当前时间为%s' % (datetime.date.today()))
                    break
                data = datas['data']['data']['Items']
                for item in data:
                    if dt_2 in item.keys():
                        split_d = item['DataTypeName']
                        if split_d == '国际价':
                            MarketSampleName = item['MarketSampleName']
                            Jg_day = item['dt_2']
                            Unit = item['Unit']
                            single_list = (MarketSampleName, Jg_day, Unit, dt_1)
                            single_lists.append(single_list)




        except Exception as e:
            print(e)

    def Province_cv(self, Province):
        # print(Province)
        if Province == '内蒙古自治区':
            return Province[0:3]
        elif Province == '广西壮族自治区':
            return Province[0:2] + '省'
        elif Province == '新疆维吾尔自治区':
            return Province[0:2]
        elif Province == '宁夏回族自治区':
            return Province[0:2]
        elif Province == '黑龙江省':
            return Province[0:3]


if __name__ == '__main__':
    a = zhuochuang()
    a.run()
    # s= '黑5555_2'
    # print(s[-1])
