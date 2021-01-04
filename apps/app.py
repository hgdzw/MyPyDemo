# 导入Flask类
from flask import Flask,render_template,request,redirect
import time
import pro
from common.mysql_operate import db


# 实例化，可视为固定格式
app = Flask(__name__)

# route()方法用于设定路由；类似spring路由配置

# 1. render_template 返回html模板方法
@app.route('/')
def index():
    return render_template("index.html")

# 2. 直接返回字符串
@app.route('/text')
def text():
    return "返回字符串格式!"

# 3. 重定向
@app.route('/redict')
def redict():
    return redirect("/")



@app.route('/login',methods=['POST']) # 设置提交方法为post
def login():
    username = request.form.get('username')
    password = request.form.get('password')
    if __name__ == '__main__':
        pro
    if username == 'admin' and password=='123456': # 登陆的账号和密码
        login_information='登陆成功'
        return render_template("index.html",login_information=login_information)
    else:
        login_information = '登陆失败'
        return render_template("index.html",login_information=login_information)

# proce
# 主方法登录
@app.route('/pro/login',methods=['GET','POST'])
def proLogin():
    username = request.form.get('username')
    password = request.form.get('password')
    proUser = pro.proTest(username,password)
    uid = proUser.get_uid()
    if uid == 0:
        #没有 要登录
        proUser.login()
    uid = proUser.get_uid()
    if uid ==0:
        return render_template("index.html",login_information="登录失败")
    return render_template("index.html",login_information="登录成功,uid={}".format(uid))

@app.route('/pro/getFile/<uid>',methods=['GET'])
def getFile(uid):
    sql = "select * from proUser where uid = '{}'".format(uid)
    result = db.select_db(sql)
    if result:
        #有值
        proUser = pro.proTest(result[0]["user_name"], result[0]["pass_word"])
        return proUser.get_count(result[0]["cookie"])

# 添加






if __name__ == '__main__':
    # app.run(host, port, debug, options)
    # 默认值：host="127.0.0.1", port=5000, debug=False
    app.run(port=80)
