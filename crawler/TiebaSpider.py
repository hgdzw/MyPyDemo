#!/usr/bin/python3
# -*- coding: utf-8 -*-
from pprint import pprint
import requests
from lxml import etree


class TiebaSpider:

    def __init__(self,kw,max_pn):
        self.base_url = "https://www.baidu.com?kw={}&ie=utf-8&pn={}"
        self.headers = {
            "User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
        }
        self.kw = kw
        self.max_pn = max_pn
        pass

    def get_url_list(self):
        '''
        url_list = []
        for pn in range(0,self.max_pn + 1,50):
            url = self.base_url.format(self.kw,pn)
            url_list.append(url)
        return url_list
        '''
        return [self.base_url.format(self.kw,pn) for pn in range(0,self.max_pn + 1,50)]

    def get_content(self,url):
        '''
        发送请求获取内容
        :param url:
        :return:
        '''
        response = requests.get(
            url=url,
            headers = self.headers
        )
        return response.content

    def get_items(self,content,idx):
        '''
        with open('13-{}.html'.format(idx),'wb') as f:
            f.write(content)

        '''

        html = content.decode('utf-8')

        eroot = etree.HTML(html)
        # print(html)

        # 提取行数据
        li_list = eroot.xpath('//li[@class="tl_shadow tl_shadow_new "]')

        for li in li_list:
            item = {}
            item["title"] = li.xpath('.//div[@class="ti_title"]/span/text()')[0].strip()
            # item["imgs"] = []
            item["imgs"] = li.xpath('.//img[@class="j_media_thumb_holder medias_img medias_thumb_holder"]/@data-url')
            # for img in img_list:
            #     print(etree.tostring(img).decode('utf-8'))
            # print(img_list)
            print(item)
            print("*"*100)
        pass

    def save_items(self,items):
        pass

    def run(self):

        # 1. 获取 url 列表
        url_list = self.get_url_list()

        for url in url_list:
            # 2. 发送请求获取响应
            content = self.get_content(url)

            # 3. 从响应中提取数据
            items = self.get_items(content,url_list.index(url) + 1)

            # 4. 保存数据
            self.save_items(items)

        pass

if __name__ == '__main__':
    spider = TiebaSpider(kw="魔兽世界",max_pn=1)
    spider.run()