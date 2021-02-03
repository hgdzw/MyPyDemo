# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from common.mysql_operate import db



SparkSession.builder.config('spark.driver.extraClassPath',
                            'D:/python/flaskDemo/sparkDemo/mysql-connector-java-5.1.46-bin.jar')

spark = SparkSession.builder \
    .appName("test") \
    .config("spark.some.config.option", "一些设置") \
    .getOrCreate()

sc = spark.sparkContext

host = '127.0.0.1'
dbName = 'mydata'
user = 'root'
passwd = '123456'

db_url = "jdbc:mysql://%s:3306/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia" \
         "/Shanghai&allowMultiQueries=true&useAffectedRows=true" % (
             host, dbName)
# 读取数据库
def queryDB(query):
    print(query)


    # host = '172.16.10.20'
    # db = 'qcplatform'
    # user = 'root'
    # passwd = 'root1234'

    df = spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", query) \
        .option("user", user) \
        .option("password", passwd) \
        .option("fetchsize", 1000000000) \
        .load()
    df.show()
    print(df.collect)
    return df

# mysql 连接数据库
def connectionMysql():
    sql = "select * from proUser"
    result = db.select_db(sql)
    df = spark.createDataFrame(result)
    print(df.printSchema())
    df.show()
    rows = df.collect()
    for i in rows:
        print(i)
    df.createOrReplaceTempView("ss")


# 测试mysql 连接
def testMysql():
    query = '''
    INSERT INTO `user`(`id`, `username`, `password`, `role`, `sex`, `telephone`, `address`) VALUES (2, '1', '2', 2, 2, '2', '2') 
    '''
    queryDB(query)
    # networkDB(query)


def run():
    df = spark.read.csv("F://data.csv", header=True, sep="|")  # 读取文件
    print(df.count())
    print(df.printSchema)


# 在内存中通过api 进行聚合
def runApi():
    print("读取json文件开始...")
    df = spark.read.json("F://prod-zhongda+0+0024526281.json")
    # 格式
    print(df.printSchema())

    # filter 过滤
    df = df.filter(df.xid > 33)

    # udf 过滤
    test_method = udf(lambda x: (x + 1), LongType())
    spark.udf.register("test_method", test_method)
    df = df.select(test_method('xid')).show()

    # 注册的时候 直接定义一个函数
    spark.udf.register("test_method2", lambda x: (x + 1))
    # 或者 指定 返回值
    spark.udf.register("test_method3", lambda x: (x + 1), LongType())

    # 将每一行拿出来
    rows = df.collect()
    # 将每个data 里面的 create_time 按天聚合  将 point_value 相加
    for i in rows:
        print(i)
    # 取第一行
    # print(df.first())
    # print(df.show())


# 在内存中通过sql 进行聚合
def runSql():
    print("读取json文件开始...")
    # 开始读取json 文件 将每一行 解析成 一个对象 然后取 第一个的 data对象
    df = spark.read.json("F://prod-zhongda+0+0024526281.json")
    # 格式
    print(df.printSchema())

    # 创建一个临时的 表 在内存中
    df.createOrReplaceTempView("data_handler")
    print("下面是进行了sql查询的")
    # 将每个data 里面的 create_time 按天聚合  将 point_value 相加
    df2 = spark.sql("select date_format(data.create_time,'y-M-d H:m') as formatStr,sum(data.point_value) from "
                    "data_handler group by formatStr")
    df2.show()

    # 将每个data 里面的 create_time 按天聚合  将 point_value 相加 第二种
    spark.udf.register("test_data_format", lambda x: x.split(":")[0] + ":" + x.split(":")[1])
    df3 = spark.sql("select *,test_data_format(data.create_time) as newTime from data_handler")
    df3.show()
    test_group = df3.groupby("newTime").agg(F.min("xid"), F.max("xid"))
    test_group = df3.groupby("newTime").agg({"xid": "max", "xid": "min"})
    test_group.show()
    test_group.join(df3, df3["newTime"] == test_group["newTime"]).show()


if __name__ == '__main__':
    # runApi()
    # runSql()
    print("这是%s" % 'aa')
    # connectionMysql()
    # testMysql()
