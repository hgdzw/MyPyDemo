# This is a sample Python script.
# 每五分钟将数据查出来清洗一下

from pyspark.sql import SparkSession
from mysql_operate import MysqlDb
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import time
import datetime

DB_HOST = 'rm-uf696403fz22dmt3f5o.mysql.rds.aliyuncs.com'
DB_USER = 'root_dev'
DB_PASS_WORD = 'Quancheng2020@'

# insert 用到
db = MysqlDb(DB_HOST, 3306, DB_USER, DB_PASS_WORD, 'history_db')

# SparkSession.builder.config('spark.driver.extraClassPath',
#                             'D:/python/flaskDemo/sparkDemo/mysql-connector-java-5.1.46-bin.jar')

spark = SparkSession.builder \
    .appName("test") \
    .config("spark.some.config.option", "一些设置") \
    .getOrCreate()

sc = spark.sparkContext


# 读取数据
def read_data():
    begin_date = datetime.datetime.now().strftime('%Y-%m-03 00:00:00')
    end_date = datetime.datetime.now().strftime('%Y-%m-03 23:59:59')
    df = get_data(begin_date, end_date)
    # 看新增还是更新
    day_df = get_data_day(begin_date, end_date)
    is_update = False
    if day_df.count() > 0:
        is_update = True
        day_df.createOrReplaceTempView("change_handler_day")

    df.createOrReplaceTempView('change_handler')
    # 分组
    group_list = spark.sql('''SELECT
                                client_id,
                                equipment,
                                point_id,
                                price_type,
                                sum( value_change ) AS value_change_total,
                                sum( electricity_value ) AS electricity_value_total 
                            FROM
                                change_handler 
                            GROUP BY
                                client_id,
                                equipment,
                                point_id,
                                price_type
                            ''')
    insert_sql = 'INSERT INTO `change_handler_day`(`client_id`, `equipment`, `point_id`, `value_change`, `window_end_time_last`, ' \
                 '`create_time`, `update_time`, `electricity_value`, `price_type`) VALUES '
    insert_if = False
    for i in group_list.collect():
        insert_if = True
        # 新增
        insert_sql = insert_sql + "('%s', '%s', '%s', %s, " \
                                  "'%s', '%s', '%s', %s, %s)," % \
                     (i['client_id'], i['equipment'], i['point_id'], i['value_change_total'],
                      begin_date, begin_date, begin_date, i['electricity_value_total'], i['price_type'],)

    # group_list.coalesce(1).write.json("F:/writeFile/day_df.json")
    if insert_if:
        if is_update:
            # 删除
            db.execute_db("delete from change_handler_day where window_end_time_last >= '%s' " \
             " and window_end_time_last <= '%s' " % (begin_date, end_date))
        insert_sql = insert_sql[:-1]
        db.execute_db(insert_sql)


def update_sql(group_list, is_update, begin_date, end_date):
    update_sql = ''
    for i in group_list.collect():
        if is_update:
            # 更新
            each = spark.sql('''select * from change_handler_day 
                where client_id = '%s' and equipment = '%s' and point_id = '%s'
                and price_type = %s''' % (i['client_id'], i['equipment'], i['point_id'], i['price_type']))

            if each.count() > 0:
                update_sql = update_sql + "update change_handler_day set value_change = '%s',electricity_value = '%s',update_time = '%s' " \
                                          "where client_id = '%s' and equipment = '%s' " \
                                          "and point_id = '%s' and price_type = %s;" % \
                             (i['value_change_total'], i['electricity_value_total'],
                              datetime.datetime.now().strftime('%Y-%m-%d, %H:%M:%S'),
                              i['client_id'], i['equipment'], i['point_id'], i['price_type'])
            else:
                insert_if = True
                insert_sql = insert_sql + "('%s', '%s', '%s', %s, " \
                                          "'%s', '%s', '%s', %s, %s)," % \
                             (i['client_id'], i['equipment'], i['point_id'], i['value_change_total'],
                              begin_date, begin_date, begin_date, i['electricity_value_total'], i['price_type'],)
        else:
            insert_if = True
            # 新增
            insert_sql = insert_sql + "('%s', '%s', '%s', %s, " \
                                      "'%s', '%s', '%s', %s, %s)," % \
                         (i['client_id'], i['equipment'], i['point_id'], i['value_change_total'],
                          begin_date, begin_date, begin_date, i['electricity_value_total'], i['price_type'],)
    # 批量更新失败  直接删除新增
    if is_update and update_sql != '':
        db.execute_db(update_sql)


# 根据指定时间 获取数据
def get_data_day(begin_date, end_date):
    sql = '''
        ( 
        select client_id,equipment,point_id,value_change,price_type,window_end_time_last
         from change_handler_day 
        where window_end_time_last >= '%s' 
             and window_end_time_last <= '%s' 
             ) temp
    ''' % (begin_date, end_date)
    return load_data(sql, 'history_db', 'client_id')


# 根据时间获取数据
def get_data(begin_date, end_date):
    sql = '''
        (
             select client_id,equipment,point_id,value_change,window_end_time_last,electricity_value,price_type 
             from change_handler 
             where price_type is not null 
             and window_end_time_last >= '%s' 
             and window_end_time_last <= '%s'
        ) temp
    ''' % (begin_date, end_date)
    return load_data(sql, 'history_db', 'client_id')


def load_data(query, db, primary_key):
    db_url = "jdbc:mysql://%s:3306/%s?useSSL=false&zeroDateTimeBehavior=convertToNull" % (DB_HOST, db)
    df = spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", query) \
        .option("user", DB_USER) \
        .option("password", DB_PASS_WORD) \
        .option("fetchsize", 1000000000) \
        .load()
    return df


if __name__ == '__main__':
    # runApi()
    read_data()
