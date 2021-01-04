
from common.mysql_operate import db
import json
import requests
LOGIN_URL = "https://www.processon.com/login"
FILE_COUNT_URL = "https://www.processon.com/login"
JAVA_GET_FILE_COUNT = "http://127.0.0.1:8088/dzw/protest/getFileCount"

class proTest():
    def __init__(self,name,passWord):
        print("创建了这个对象")
        self.name = name
        self.passWord = passWord

    def get_uid(self):
        sql = "select * from proUser where user_name = '{}'".format(self.name)
        result = db.select_db(sql)
        print("获取 {} 用户信息 == >> {}".format(self.name, result))
        if result:
            return result[0]["uid"]
        return 0

    def login(self):
        cookie = self.get_cookie()
        print("登录成功,写入数据库...cookie"+cookie)
        inserSql = "INSERT INTO `prouser`(`user_name`, `pass_word`, `cookie`) VALUES ( '{}', '{}', '{}');".format(self.name,self.passWord,cookie)
        db.execute_db(inserSql)


    def get_cookie(self):
        cookie = ""
        print("登录中...")
        hearad = {
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-length": "32",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-fetch-mode": "cors",
        }
        data = {
            "login_email": self.name,
            "login_password": self.passWord,

        }
        response = requests.post(url=LOGIN_URL, params=data, allow_redirects=False)
        for item in response.cookies.items():
            cookie = cookie + str(item[0]) + '=' + str(item[1]) + ';'
        print("登录成功..用户名{},cookie={}".format(self.name,cookie))
        return cookie

    #cookie 失效
    def exp_cookie(self):
        cookie = self.get_cookie()
        sql = "update proUser set cookie = '{}' where user_name = '{}'".format(cookie,self.name)
        db.execute_db(sql)
        return cookie

    def count(self,cookie):
        data = {
            "cookie": str(cookie)
        }
        response = requests.post(url=JAVA_GET_FILE_COUNT, data=data).text
        jsonStr = json.load(response)


    # 获取文件数
    def get_count(self, cookie):
        data = {
            "cookie": cookie
        }
        response = requests.post(url=JAVA_GET_FILE_COUNT, data=data).text
        jsonStr = json.load(response)
        # jsonStr = self.count(str(cookie))
        if jsonStr["data"] is None:
            # cookie 失效
            cookie = self.exp_cookie()
            jsonStr = self.count(cookie)
        total = jsonStr["data"]["totalcount"]
        file = jsonStr["data"]["filecount"]
        if total is None or file is None:
            # 如果是获取不到
            return -1
        print(total-file)
        return total-file

if __name__ == '__main__':
    pro = proTest("2311839009@qq.com","zheshimima")
    pro.login()