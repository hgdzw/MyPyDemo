#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time


class HttpCrawler:
    def __init__(self):
        self.base_url = "https://www.bjets.com.cn/article/jyxx/?{}"
        self.headers = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        pass

    def chongqing_list(self):
        url = 'https://tpf.cqggzy.com/itf/themes/cqkfts/KjaxAjaxServlet.as'

        xml = ' <listbex><bex func="f_itf_cqhq_get"><wsBexInstance>'+str(time.time())+'</wsBexInstance></bex></listbex>'

        hearder = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }

        response = requests.post(url,headers = hearder,data=xml.encode('utf-8')).text

        items = []
        try:
            json_list = json.loads(response)["f_itf_cqhq_get"]["data"][0]["records"]
            for data in json_list:
                item = {}

                timeStr = str(data["fsrq"])

                item["time"] = timeStr[0:4] + '-' + timeStr[4:6] + '-' + timeStr[6:8]
                item["type"] = str(data["pzmc"]).strip()
                item["avg"] = data["aver"]
                item["count"] = data["cjsl"]
                if '0' == data["cjsl"]:
                    # 成交量为0的过滤
                    continue
                if int(timeStr[0:4]) < 2020:
                    # 只取 2020年开始的数据
                    continue
                item["total"] = '%.2f' %(float(data["cjsl"]) * float(data["aver"]))
                items.append(item)

        except BaseException:
            print("出现错误!解析异常！")

        # 把所有items保存到文件中
        with open('data/chongqing-data.json','w',encoding='utf-8') as f:
            json.dump(items,f,ensure_ascii=False,indent=4)

        pass


    def shanghai_list(self):
        start = 2020
        now_year = time.strftime("%Y", time.localtime())
        items = []
        [self.shanghai_html(i,items) for i in range(start,int(now_year)+1,1)]
        return items


    def shanghai_html(self,year,items):

        request_body  = MultipartEncoder({
            "Date": str(year),
            "Type":"YEAR"
        })
        request_header = {
            "Content-Type": request_body.content_type
        }

        response = requests.post("https://www.cneeex.com/cneeex/daytrade/selectData",headers=request_header,data=request_body)

        json_list = json.loads(response.text)
        for data in json_list:
            try:
                item = {}
                item["time"] = data[0]
                item["type"] = data[1]
                item["count"] = str(data[2]).replace(',','')
                if '-' == data[2]:
                    # 成交量为0的过滤
                    continue
                item["total"] = str(data[3]).replace(',','')
                item["avg"] = '%.2f' %(float(item["total"]) / float(item["count"]))
                items.append(item)

            except BaseException:
                print("解析上海数据异常!")




        # # 把所有items保存到文件中
        # with open('data/shanghai-data.json','w',encoding='utf-8') as f:
        #     json.dump(items,f,ensure_ascii=False,indent=4)
        #
        # pass

if __name__ == '__main__':
    spider = HttpCrawler()
    spider.shanghai_list()