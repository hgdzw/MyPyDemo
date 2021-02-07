# This is a sample Python script.


from pyspark.sql import SparkSession
from common.mysql_operate import db
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import time
import datetime
import json

SparkSession.builder.config('spark.driver.extraClassPath',
                            'D:/python/flaskDemo/sparkDemo/mysql-connector-java-5.1.46-bin.jar')


spark = SparkSession.builder \
    .appName("test") \
    .config("spark.some.config.option", "一些设置") \
    .config("master"," local[*]")   \
    .getOrCreate()

sc = spark.sparkContext

# 格式化时间
def time_format(timeStr):
    minutes = timeStr.split(':')[1]
    minutes = int(minutes)
    minutes = int(minutes / 5) * 5
    if minutes < 10:
        minutes = '0'+str(minutes)
    return timeStr.split(':')[0] + ':' + str(minutes) + ':00'

# 读取数据
def read_data():
    # df = spark.read.json("F:\data\prod-zhongda+0+0024526281.json")
    df = spark.read.json("F:/data/prod-zhongda+11+0034993222.json")
    # df = spark.read.json("F:/data/*.json")
    # df = spark.read.json("oss:/*.json")

    df = df.select(
        df.data.client_id.alias('client_id'),
        df.data.point_id.alias('point_id'),
        df.data.update_time.alias('update_time'),
        df.data.point_value.alias('point_value'),
        df.xid.alias('xid'),
        df.data.equipment.alias('equipment'))
    # df.show()
    df.printSchema()
    print(df.count())
    df = df.filter("update_time> '2021-01-01 00:00:00'")
    # 过滤 只有电的
    df = df.filter("point_id == 'ACC61001' or point_id == 'ACC62001' or point_id == 'ACC61002' or point_id == 'ACC62002'")

    # 将列表中的data 拿出来 然后 按照 point_id client_id equipment 分组 一天的数据 聚合成一条
    df.createOrReplaceTempView("data")

    # # 分组聚合
    # group = df.groupby("point_id", "equipment", "client_id").agg(F.sum("point_value"))
    # print("第一种分组聚合条数%s" % group.count())
    #
    # # sql聚合
    # sql_group = spark.sql("select point_id,equipment,client_id,sum(point_value) as total from data group by point_id,"
    #                       "equipment,client_id")
    # print("第二种sql聚合条数%s" % sql_group.count())

    # 五分钟分组聚合
    # time_format = udf(lambda x: x + 1, LongType())
    # spark.udf.register("time_format", udf(time_format))
    # minutes_df = spark.sql("select time_format(update_time) as time,point_id,equipment,client_id,sum(point_value) "
    #                        "from data group by point_id,equipment,client_id,time")
    # print('五分钟聚合得到的数据条数%s' % minutes_df.count())
    # minutes_df.show()
    # minutes_df.write.json("F:/writeFile/minutes_df.json")

    # 按天聚合
    day_df = spark.sql("select DATE_FORMAT(update_time,'yyyy-MM-dd') as time,point_id,equipment,client_id,sum(point_value) "
                           "from data group by point_id,equipment,client_id,time")
    print('按天聚合得到的数据条数%s' % day_df.count())

    # 聚合成少的
    day_df.coalesce(1).write.json("F:/writeFile/day_df.json")
    # # 文件变多的
    # day_df.repartition(1000).write.json("F:/writeFile/day_df2.json")

    sql = ''
    for i in day_df.collect():
        print(i)


    df.show()
    print(df.count())


# 读取数据库

def query_DB(query):
    print(query)

    host = '172.16.10.20'
    dbName = 'qcplatform'
    user = 'root'
    passwd = 'root1234'
    db_url = "jdbc:mysql://%s:3306/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia" \
             "/Shanghai&allowMultiQueries=true&useAffectedRows=true" % (
                 host, dbName)

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
def clean_data():
    sql = "( select * from data_handler where update_time > '2020-10-01' ORDER BY update_time desc ) temp"
    result = db.select_db(sql)
    df = spark.createDataFrame(result)
    print(df.printSchema())
    df.show()
    rows = df.collect()
    for i in rows:
        print(i)
    df.createOrReplaceTempView("ss")


def test_time():
    dt = datetime.datetime.strptime('20200201121412', '%Y%m%d%H%M%S')
    time_3 = datetime.datetime(2020,10,11,0,0,0)
    now_date = datetime.datetime.now()
    print(now_date.year)
    print(now_date.month)
    print(now_date.day)
    print(now_date.hour)
    print(now_date.minute)
    print(now_date.microsecond)

    print(dt.strftime('%Y%m%d'))

    print(type(time_3))
    print(time_3)
    print(type(time.localtime()))
    print(time.localtime())
    print(type(datetime.datetime.now()))
    print(datetime.datetime.now())
    print(dt+datetime.timedelta(days=-1))
    print(datetime.date(time.localtime()))


    # time_str2 = (+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
    # time_str = time.localtime().tm_min
    # print(time_str2)

def test_time2():
    begin_date = datetime.datetime.now().strftime('%Y-%m-%d 00:00:00')
    end_date = datetime.datetime.now().strftime('%Y-%m-%d 23:59:59')
    print(begin_date[:-1])
    print(end_date)

    jsonData = '{"a":1,"b":2,"c":3,"d":4,"e":5}'

    text = json.loads(jsonData)
    print(text)


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


if __name__ == '__main__':
    print(last_day_of_month(datetime.datetime(2021, 1, 1)))
    # runApi()
    # test_time()
    test_time2()
    # print(time_format_3('2020-01-02 01:02:03'))
    # connectionMysql()
    # testMysql()
