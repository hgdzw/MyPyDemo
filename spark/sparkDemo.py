# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.



from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder\
        .appName("test") \
        .config("spark.some.config.option", "一些设置") \
        .getOrCreate()

    df = spark.read.csv("F://data.csv", header=True, sep="|")  # 读取文件
    
    print(df.count())


if __name__ == '__main__':
    run()