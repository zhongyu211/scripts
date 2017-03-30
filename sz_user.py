# coding:utf-8
# __author__ = 'allen zhong, mei'
import os
import re
import sys
import getopt
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SQLContext, DataFrame, Row, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from math import ceil
import traceback
from random import randint

from pytz import timezone

import time

import socket

# global configuration for spark
spark = SparkSession.builder\
                    .appName("sz_user")\
                    .config("spark.driver.maxResultSize", "10g")\
                    .getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def is_valid_ipv4_address(address):
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  # no inet_pton here, sorry
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:  # not a valid address
        return False

    return True

def get_rand_ip(start, end):
    if start.__class__.__name__ =='unicode' and end.__class__.__name__ =='unicode':
        if not (is_valid_ipv4_address(start) and is_valid_ipv4_address(end)):
            start ='224.0.0.0'
            end = '255.255.255.255'
    else:
        start ='224.0.0.0'
        end = '255.255.255.255'    
    start_list = map(lambda x: int(x), start.split('.'))
    end_list = map(lambda x: int(x), end.split('.'))
    res_ip = str(randint(start_list[0], end_list[0])) + '.' + str(randint(start_list[1], end_list[1])) + \
        '.' + str(randint(start_list[2], end_list[2])) + \
        '.' + str(randint(start_list[3], end_list[3]))
    return res_ip

ip_f = F.udf(get_rand_ip, StringType())

def local_check(start, end):
    res = False
    if start:
        startlist = start.split('.')
        if not startlist[0] in ['10','172','192']:
            res = True
    return res

check_f = F.udf(local_check, BooleanType())


def get_subnet_df():
    data_path = "s3n://allen-spark-test/sz_user/ipsubnet"
    df = sc.textFile(data_path)\
        .map(lambda x: Row(start=x.split('|')[0], end=x.split('|')[1], country_name=x.split('|')[11]))\
        .toDF()\
        .filter(check_f('start', 'end'))\
        .select('country_name', 'start', 'end')\
        .distinct()
    window = Window.partitionBy(df.country_name).orderBy(df.start)
    # F.row_number().over(window).alias('rank')).distinct().filter('rank = 1')
    df=df.select('country_name', 'start', 'end', F.row_number().over(window).alias('rank'))\
         .filter('rank=1')\
         .select('country_name','start', 'end')
    return df


def get_data_df():
    data_path = "s3n://allen-spark-test/sz_user/users000"
    df = sc.textFile(data_path)\
        .map(lambda x: Row(game_id=x.split(',')[0], uid=x.split(',')[1], data_desc=x.split(',')[2],
                           register_time=x.split(
                               ',')[3], final_source=x.split(',')[4],
                           is_tutorial=x.split(',')[5], country_name=x.split(',')[6]))\
        .toDF()

    print df.count()
    print df.take(5)
    return df

def process_ip():
    df1 = get_data_df()
    df2 = get_subnet_df()
    dfjoin =df1.join(df2,df1.country_name == df2.country_name,'left_outer')\
               .select(df1.game_id, df1.uid, df1.data_desc, df1.register_time, df1.final_source,
                      df1.is_tutorial, df1.country_name, df2.start, df2.end)
    print dfjoin.count()
    df = dfjoin.select('game_id', 'uid', 'data_desc', 'register_time', 'final_source',
                       'is_tutorial', 'country_name', ip_f('start', 'end').alias('ip'))
    df.write.mode('overwrite').csv("s3n://allen-spark-test/sz_user/result/ip")
    #print dfjoin.select('country_name', 'start','end').filter(F.isnull('start')).distinct().collect()


if __name__ == '__main__':
    process_ip()
