#!/usr/bin/python3
# -*- coding: utf-8 -*-

from crawler.BeiJingCarbon import BeiJingCarbon
from crawler.HttpCrawler import HttpCrawler

if __name__ == '__main__':

    # 北京
    spider = BeiJingCarbon()
    items = spider.run()


    # 上海
    spider = HttpCrawler()
    spider.shanghai_list()


