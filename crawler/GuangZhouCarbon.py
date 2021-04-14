#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import re
import time


class GuangZhouCarbon:

    def __init__(self):
        self.base_url = "http://ets.cnemission.com/carbon/portalIndex/markethistory?Top=1&beginTime=2020-01-01" \
                        "&endTime={} "
        self.headers = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        pass

    def get_url_list(self):
        now = time.strftime("%Y-%m-%d", time.localtime())
        return [self.base_url.format(now)]

    def get_html(self,url):
        response = requests.get(url,headers=self.headers)
        response.encoding = "utf-8"
        return response.text

    def get_items(self,html):
        items = []
        soup = BeautifulSoup(html,'lxml')
        tr_list = soup.select('.lh_baseTable tr')
        for tr in tr_list[1:]:
            item = {}
            td_list = tr.find_all('td')
            item["time"] = td_list[0].get_text()[:4]+'-'+td_list[0].get_text()[4:6]+'-'+td_list[0].get_text()[6:8]
            item["count"] = td_list[8].get_text().strip()
            item["avg"] = '%.2f' %(float(td_list[9].get_text()) / float(td_list[8].get_text()))
            item["total"] = td_list[9].get_text().strip()
            item["type"] = td_list[1].get_text().strip()
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
        with open('data/guangzhou-data.json','w',encoding='utf-8') as f:
            json.dump(all_items,f,ensure_ascii=False,indent=4)

        pass


if __name__ == '__main__':
    spider = GuangZhouCarbon()
    spider.run()