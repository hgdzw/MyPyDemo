#!/usr/bin/python3
# -*- coding: utf-8 -*-
from crawler.BeiJingCarbon import BeiJingCarbon
from crawler.GuangZhouCarbon import GuangZhouCarbon
from crawler.HttpCrawler import HttpCrawler
from crawler.HuBeiCarbon import HuBeiCarbon
from crawler.ShenZhenCarbon import ShenZhenCarbon
from crawler.TianJinCarbon import TianJinCarbon
import time
import requests
from crawler.mysql_operate import MysqlDb

DB_HOST = '172.16.20.34'
DB_USER = 'root'
DB_PASS_WORD = 'root1234'


db = MysqlDb(DB_HOST, 3306, DB_USER, DB_PASS_WORD, 'qcplatform')


def build_sql(items, data, city_name, city_code):
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for item in items:
        total = str(item["total"]).replace(',', '')
        avg = str(item["avg"]).replace(',', '')
        count = str(item["count"]).replace(',', '')
        data.append((city_name, city_code, item["time"], item["type"], avg, total, count, now,
                     avg, total, count, now))


def send_error_msg(city_name):
    url = 'https://oapi.dingtalk.com/robot/send?access_token=a0e3e44c816d6ab6f126f5488ba6567829587b33eee1cc9dad63c320d2659cec'

    data = {
        "msgtype": "text",
        "text": {
            "content": "qc-碳排放数据抓取出现错误,城市:"+city_name
        }
    }

    hearder = {
        "Content-Type": "application/json"
    }

    response = requests.post(url=url, headers=hearder, json=data)
    print(response.text)
    print("出现错误"+city_name)


if __name__ == '__main__':
    template = "insert INTO `carbon_emissions_data`( " \
               "`city_name`, `city_code`, `time`, `trading_type`, `average`, `turnover`, `volume`, " \
               "`remarks`, `create_time`, `create_by`, `available`) VALUES (" \
               "%s, %s, %s, %s, %s, %s, %s, " \
               "NULL, %s, 'admin', 1) on DUPLICATE key  UPDATE  average= %s,turnover=%s," \
               "volume=%s,update_time = %s,update_by = 'admin'; "
    data_list = []

    # 北京
    spider = BeiJingCarbon()
    beijing = spider.run()
    build_sql(beijing, data_list,'北京','110100')
    try:
        db.execute_many(template,data_list)
    except Exception as e:
        send_error_msg('北京')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理北京数据完成---")

    # 广州
    guangzhou_list = []
    spider = GuangZhouCarbon()
    guangzhou = spider.run()
    build_sql(guangzhou, guangzhou_list, '广州', '440100')
    try:
        db.execute_many(template,guangzhou_list)
    except Exception as e:
        send_error_msg('广州')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理广州数据完成---")

    # 天津
    tianjin_list = []
    spider = TianJinCarbon()
    tianjin = spider.run()
    build_sql(tianjin, tianjin_list, '天津', '120100')
    try:
        db.execute_many(template,tianjin_list)
    except Exception as e:
        send_error_msg('天津')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理天津数据完成---")

    # 深圳
    shenzhen_list = []
    spider = ShenZhenCarbon()
    shenzhen = spider.run()
    build_sql(shenzhen, shenzhen_list, '深圳', '440300')
    try:
        db.execute_many(template,shenzhen_list)
    except Exception as e:
        send_error_msg('深圳')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理深圳数据完成---")

    # 湖北
    hubei_list = []
    spider = HuBeiCarbon()
    hubei = spider.run()
    build_sql(hubei, hubei_list, '湖北', '430014')
    try:
        db.execute_many(template,hubei_list)
    except Exception as e:
        send_error_msg('湖北')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理湖北数据完成---")

    # # 上海
    shanghai_list = []
    spider = HttpCrawler()
    shanghai = spider.shanghai_list()
    build_sql(shanghai, shanghai_list, '上海', '310100')
    try:
        db.execute_many(template, shanghai_list)
    except Exception as e:
        send_error_msg('上海')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理上海数据完成---")

    # 重庆
    chongqing_list = []
    chongqing = spider.chongqing_list()
    build_sql(chongqing, chongqing_list, '重庆', '500100')
    try:
        db.execute_many(template,chongqing_list)
    except Exception as e:
        send_error_msg('重庆')
        print("操作出现错误：,错误原因{}".format(e))
    print("处理重庆数据完成---")


