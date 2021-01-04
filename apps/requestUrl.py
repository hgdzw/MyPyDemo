# 导入Flask类
from flask import Flask, render_template, request, redirect
import time
import requests

def requestTest():
    url = 'http://127.0.0.1:8183/inspection/saveInspectionPlan?files'
    requests.get(url, )
    headers = {'Accept': 'text/html, application/xhtml+xml, image/jxr, */*',
               'Accept - Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-Hans-CN, zh-Hans; q=0.5',
               'Connection': 'Keep-Alive',
               'Host': 'zhannei.baidu.com',
               'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXJyZW50VGltZU1pbGxpcyI6IjE2MDg4ODM3NTg2NjAiLCJleHAiOjE2MDg5NzAxNTgsImFjY291bnQiOiJkb25nemhpd2VpIn0.4IXWGY1Gc0imgEmxtiyWZ6q9VEpdkhwFDuvtgxh4zpM',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'}
    data = {

        "clientId": "5495a0e0-2a8c-467b-b128-31093b0d225f",
        "siteBaseId": "dd6e8a56-879e-4f79-a5a5-8c02737e7155",
        "teamId": "3fda976c-462d-4114-bb41-caaf33695764",
        "firstTaskTime": "2021-01-07 00:00:00",
        "repeatRate": 1,
        "planDesc": 123456

    }


    data2 = {
        "siteId":"dd6e8a56-879e-4f79-a5a5-8c02737e7155",
        "loopType":"1"
    }

    dd = requests.get('http://www.baidu.com')
    print(dd.text)
    # r = requests.post(url,data=data,headers = headers)
    r = requests.post(url=url, data=data, headers={'Content-Type':'application/x-www-form-urlencoded'})
    print(r.text)

def test2():
    url = "http://127.0.0.1:8183/electricity/listLoopBySiteId"
    data2 = {
        "siteId": "dd6e8a56-879e-4f79-a5a5-8c02737e7155",
        "loopType": "1"
    }
    helder = {
        'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXJyZW50VGltZU1pbGxpcyI6IjE2MDg4ODM3NTg2NjAiLCJleHAiOjE2MDg5NzAxNTgsImFjY291bnQiOiJkb25nemhpd2VpIn0.4IXWGY1Gc0imgEmxtiyWZ6q9VEpdkhwFDuvtgxh4zpM',
    }

    res = requests.post(url=url,data=data2,headers=helder)
    print(res.text)

def test_case1():
    # url = "http://172.16.20.39:8183/user/login"
    # userinfo = {"account": "dzw", "passWord": "12345678"}
    # s = requests.session()
    # response = s.post(url, json=userinfo).text
    # print(response)
    # global null
    # null = ''
    # false = False
    # true = True
    # dic = {}
    # dic = eval(response)
    # token = dic["data"]["token"]
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXJyZW50VGltZU1pbGxpcyI6IjE2MDg4ODM3NTg2NjAiLCJleHAiOjE2MDg5NzAxNTgsImFjY291bnQiOiJkb25nemhpd2VpIn0.4IXWGY1Gc0imgEmxtiyWZ6q9VEpdkhwFDuvtgxh4zpM"

    url = "http://172.16.20.39:8183/inspection/saveInspectionPlan"
    userinfo = {
        "clientId": "5495a0e0-2a8c-467b-b128-31093b0d225f",
        "siteBaseId": "dd6e8a56-879e-4f79-a5a5-8c02737e7155",
        "teamId": "3fda976c-462d-4114-bb41-caaf33695764",
        "firstTaskTime": "2025-01-07 00:00:00",
        "repeatRate": 1,
        "planDesc": 123456
    }
    headers = {"Authorization": token,
                "Content-Length":"1000",
               "User-Agent":"PostmanRuntime/7.26.8",
               "Accept-Encoding":"gzip, deflate, br",
               "Connection":"keep-alive"

               }
    files = {
        "files":'dqwe'
    }
    response = requests.post(url= url,files= files, data=userinfo, headers=headers).text

    print(response)



if __name__ == '__main__':
    test_case1()
