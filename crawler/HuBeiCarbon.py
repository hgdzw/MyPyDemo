#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import re
import time


class HuBeiCarbon:

    def __init__(self):
        self.base_url = "http://www.hbets.cn/list/13.html?page={}"

        self.headers = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        pass

    def get_url_list(self):
        # return [self.base_url.format(start) for start in range(1,14,1)]
        return [self.base_url.format(start) for start in range(1,2,1)]

    def get_html(self,url):
        response = requests.get(url,headers=self.headers)
        response.encoding = "utf-8"
        return response.text

    def get_items(self,html):
        items = []
        soup = BeautifulSoup(html,'lxml')
        tr_list = soup.select('.future_table  ul')
        for tr in tr_list[1:]:
            item = {}
            td_list = tr.find_all('li')
            item["time"] = td_list[1].get_text()
            item["type"] = td_list[0].get_text()
            if int(td_list[1].get_text()[:4]) < 2020:
                break
            item["count"] = td_list[6].get_text()
            item["avg"] = '%.2f' %(float(td_list[7].get_text()) / float(td_list[6].get_text()))
            item["total"] = td_list[7].get_text()
            items.append(item)
        return items

    def save_item(self,item):
        print(item)

    def run(self):
        # 1. 获取 url 列表
        url_list = self.get_url_list()

        all_items = []
        for url in url_list:
            # 2. 发送请求获取html
            html = self.get_html(url)

            # 3. 通过 html 提取数据
            items = self.get_items(html)

            # 4. 保存数据
            for item in items:
                self.   save_item(item)
                all_items.append(item)
                # break

        # 把所有items保存到文件中
        with open('data/hubei-data.json','w',encoding='utf-8') as f:
            json.dump(all_items,f,ensure_ascii=False,indent=4)

        pass


if __name__ == '__main__':
    spider = HuBeiCarbon()
    spider.run()