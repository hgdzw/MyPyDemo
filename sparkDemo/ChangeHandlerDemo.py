# This is a sample Python script.

from pyspark.sql import SparkSession
from common.mysql_operate import db
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import time
import datetime

DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASS_WORD = '123456'


SparkSession.builder.config('spark.driver.extraClassPath',
                            'D:/python/flaskDemo/sparkDemo/mysql-connector-java-5.1.46-bin.jar')


spark = SparkSession.builder \
    .appName("test") \
    .config("spark.some.config.option", "一些设置") \
    .getOrCreate()

sc = spark.sparkContext


# 格式化时间
def time_format(timeStr):
    minutes = timeStr.split(':')[1]
    minutes = int(minutes)
    minutes = int(minutes / 5) * 5
    if minutes < 10:
        minutes = '0' + str(minutes)
    return timeStr.split(':')[0] + ':' + str(minutes) + ':00'


# 时间减去五分钟
def time_sub(timeStr):
    time_time = datetime.datetime.strptime(timeStr, '%Y-%m-%d %H:%M:%S')
    # 偏移五分钟
    return (time_time + datetime.timedelta(minutes=-5)).strftime("%Y-%m-%d %H:%M:%S")


# 读取数据
def read_data():
    # 厂站id
    site_id = ''
    df = spark.read.json("F:/data/*.json")
    df = df.select(
        df.xid.alias('xid'),
        df.data.client_id.alias('client_id'),
        df.data.point_id.alias('point_id'),
        df.data.equipment.alias('equipment'),
        df.data.push_time.alias('push_time'),
        df.data.point_value.alias('point_value')
    )

    df.printSchema()
    # 过滤 只有电的
    df = df.filter("point_id == 'ACC61001' or point_id == 'ACC62001' or "
                   "point_id == 'ACC61002' or point_id == 'ACC62002'")
    print('查询出数目总条数为%s' % df.count())

    # 查询两天的数据 最后聚合成一天的数据总数  和 最近的五分钟数据
    # begin_now = time.strftime("%Y-%m-%d 00:00:00", time.localtime())
    begin_now = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d 00:00:00")
    # df = df.filter(df.push_time > begin_now)
    df.createOrReplaceTempView("data")

    # 将一天的按照五分钟聚合
    spark.udf.register("time_format", udf(time_format))
    spark.udf.register("time_sub", udf(time_sub))
    minutes_df = spark.sql("select time,time_sub(time) as sub_time,client_id,point_id,equipment,max_value"
                           " from (select time_format(push_time) as time,point_id,equipment,client_id,"
                           "max(point_value) as max_value from data group by point_id,equipment,client_id,time)")
    minutes_df.createOrReplaceTempView("now_data")
    minutes_df.createOrReplaceTempView("now_data_two")

    # print('按五分钟聚合得到的数据条数%s' % minutes_df.count())

    # 关联查询
    left_df = spark.sql('''select 
                                n.client_id,
                                n.point_id,
                                n.equipment,
                                n.time,
                                n.max_value as value_latest,
                                nd.max_value as value_last,
                                (nd.max_value - n.max_value) as value_change
                        from now_data n left join now_data_two nd 
                        on n.time = nd.sub_time and n.client_id = nd.client_id 
                        and n.point_id = nd.point_id and n.equipment = nd.equipment
    ''')
    left_df.coalesce(1).write.json("F:/writeFile/day_df.json")

    get_plan_config_list(site_id,)


# 根据厂站 获取指定时间的配置
def get_plan_config_list(siteId, startDate, endDate, month):
    sql = '''      
       (SELECT
            hs.effective_begin_time,
            hs.effective_end_time,
            hs.template_id,
            epd.`month`,
            epd.summer_type,
            epd.price_type_value,
            epc.interval_type,
            epc.summer_price_peak,
            epc.summer_price_flat,
            epc.summer_price_valley,
            epc.summer_price_tip,
            epc.other_price_peak,
            epc.other_price_flat,
            epc.other_price_valley,
            epc.other_price_tip 
        FROM
            history_snapshot hs
            LEFT JOIN electricity_plan_config epc ON hs.template_id = epc.id 
            AND epc.available = 1
            LEFT JOIN electricity_plan_detail epd ON epc.id = epd.config_id 
            AND epd.available = 1 
        WHERE
            hs.available = 1 
            and hs.site_id = '%s'
            AND hs.type = 2 
            AND epd.`month` = %s
            AND (
            ( hs.effective_begin_time <= '%s' AND hs.effective_end_time > '%s' ) 
            OR ( hs.effective_begin_time <= '%s' AND hs.effective_end_time > '%s' ) 
            )
            ) temp
    ''' % (siteId, month, startDate, startDate, endDate, endDate)
    # return db.select_db(sql)
    return load_data(sql, 'qcplatform')


def load_data(query, db):
    # query = "(select * from dim_kpi) dim_kpi"
    # conn_db = BaseHook.get_connection('mysql_report_kpi_db')
    db_url = "jdbc:mysql://%s:3306/%s?useSSL=false&zeroDateTimeBehavior=convertToNull" % (DB_HOST, db)
    df = spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", query) \
        .option("user", DB_USER) \
        .option("password", DB_PASS_WORD) \
        .option("fetchsize", 1000000000) \
        .option("partitionColumn", "id") \
        .option("lowerBound", 10) \
        .option("upperBound", 10) \
        .option("numPartitions", 10) \
        .load()
    return df


if __name__ == '__main__':
    # runApi()
    read_data()
    # sql = '(select * from philosophy) temp'
    # df = load_data(sql, 'mydata')
    # print(time_format_3('2020-01-02 01:02:03'))
    # connectionMysql()
    # testMysql()




