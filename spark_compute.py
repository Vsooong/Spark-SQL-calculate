#!/usr/bin/env python
# coding: utf-8


import datetime
import numpy as np
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce
import datetime

def build_shema():
    column_names = ['day_id', 'calling_nbr', 'called_nbr', 'calling_optr', 'called_optr','calling_city', 'called_city', 'calling_roam_city', 'called_roam_city', 'start_time','end_time', 'raw_dur', 'call_type', 'calling_cell']
    column_type=['STRING']*11+['INT']+['STRING']*2
    len_col=len(column_type)
    schema=[StructField(column_names[i], StringType() if column_type[i]=='STRING' else IntegerType(), True) for i in range(len_col)]
    schema=StructType(schema)
    return schema

def time_distribute_udf(time_id, start, end, raw_dur):
    start = datetime.datetime.strptime(start, "%H:%M:%S")
    end = datetime.datetime.strptime(end, "%H:%M:%S")
    invalid_time = datetime.time(0, 0, 0)
    res = 0
    if end.time() == invalid_time:
        end = start + datetime.timedelta(seconds=raw_dur)
    elif start.time() == invalid_time:
        end = end + datetime.timedelta(days=1)
        start = end - datetime.timedelta(seconds=raw_dur)
    if start >= end:
        end = end + datetime.timedelta(days=1)
    start_time = datetime.datetime(1900, 1, 1, (time_id - 1) * 3, 0, 0)
    def _get_period_in_one_day(start_time):
        end_time = start_time + datetime.timedelta(hours=3)
        if start >= end_time or end < start_time:
            return 0
        else:
            from_start = (start - start_time).seconds if start >= start_time else 0
            from_end = (end_time - end).seconds if end < end_time else 0
            return 3 * 60 * 60 - from_start - from_end
    res += _get_period_in_one_day(start_time)
    start_time = datetime.datetime(1900, 1, 2, (time_id - 1) * 3, 0, 0)
    res += _get_period_in_one_day(start_time)
    return res

def qes_1(spark):
    print("Start computing question 1：")
    qes1=spark.sql("""
    select calling_nbr, sum(call_num)/count(1) as avg_calling
    from 
    (
        select calling_nbr, day_id as day,count(1) as call_num
        from calling
        group by calling_nbr,day_id
    )
    group by calling_nbr
    """)
    qes1.write.csv("dc2019/qes_1.csv",mode='overwrite')
    print("Question 1 completed")

    
def qes_2(spark):
    print("Start computing question 2：")
    qes2=spark.sql("""
    select
    ta.call_type,
    called_optr,
    user_num/user_num_2*100 as percent
    from (
        select call_type,called_optr, count (distinct called_nbr) as user_num
        from calling
        group by call_type,called_optr
    )as ta join
    (
        select call_type,count(distinct called_nbr) as user_num_2
        from calling
        group by call_type
    )as tb 
    on (ta.call_type=tb.call_type)
    order by ta.call_type,called_optr
    """)
    qes2.write.csv("dc2019/qes_2.csv",mode='overwrite')
    print("Question 2 completed")


def qes_3(spark):
    print("Start computing question 3：")
    qes3=spark.sql("""
    select
    calling_nbr,
    100*period_1/total_call as period_1,100*period_2/total_call as period_2,
    100*period_3/total_call as period_3,100*period_4/total_call as period_4,
    100*period_5/total_call as period_5,100*period_6/total_call as period_6,
    100*period_7/total_call as period_7,100*period_8/total_call as period_8
    from 
    (
        select
        calling_nbr,
        sum(period_1) as period_1,sum(period_2) as period_2,
        sum(period_3) as period_3,sum(period_4) as period_4,
        sum(period_5) as period_5,sum(period_6) as period_6,
        sum(period_7) as period_7,sum(period_8) as period_8,
        sum(raw_dur) as total_call
        from 
        (
            select calling_nbr,
            time_distribute(1,start_time,end_time,raw_dur) as period_1,time_distribute(2,start_time,end_time,raw_dur) as period_2,
            time_distribute(3,start_time,end_time,raw_dur) as period_3,time_distribute(4,start_time,end_time,raw_dur) as period_4,
            time_distribute(5,start_time,end_time,raw_dur) as period_5,time_distribute(6,start_time,end_time,raw_dur) as period_6,
            time_distribute(7,start_time,end_time,raw_dur) as period_7,time_distribute(8,start_time,end_time,raw_dur) as period_8,
            raw_dur
            from calling
            where raw_dur!=0
        ) 
        group by calling_nbr
    )
    """)
    qes3.write.csv("dc2019/qes_3.csv",mode='overwrite')
    print("Question 3 completed")


# In[53]:


if __name__=='__main__':
    local = False
    MASTER = "local[*]" if local else "spark://10.60.43.111:7077"
    spark = SparkSession.builder.master(MASTER).appName('DC3p').getOrCreate()
    data_path='hdfs://cluster01:8020/user/liaoshanhe/tb_call_201202_random-2018.txt'
    df = spark.read.csv(data_path, sep='\t',schema=build_shema())
    df.registerTempTable('calling')
    spark.udf.register('time_distribute',time_distribute_udf,IntegerType())
    qes_1(spark)
    qes_2(spark)
    qes_3(spark)

