# -*- coding: utf-8 -*-
"""edit-bigdata HW4_Draft.ipynb
"""

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)
from numpy import std
from numpy import median
import datetime
import json
import csv

if __name__=='__main__':
    core = 'hdfs:///data/share/bdm/core-places-nyc.csv'
    nyc_rest = 'hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*'

    #def funtions:
    def geteveryday(x):
        date_list = []
        begin_date = datetime.datetime.strptime(x[1][:10], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(x[2][:10], "%Y-%m-%d")
        while begin_date + datetime.timedelta(days=1) <= end_date:
            date_str = str(begin_date.year) + '-' + str(begin_date.month) + '-' + str(begin_date.day)
            date_list.append(date_str)
            begin_date += datetime.timedelta(days=1)
        return (x[0],x[3],date_list)

    def combine(x):
      combinelist=zip(x[2],json.loads(x[1]))
      return tuple(combinelist)

    coreid=sc.textFile(core)\
             .map(lambda x: next(csv.reader([x])))\
             .map(lambda x: (x[1], x[9]))\
             .cache()

    rest_list=set(coreid.filter(lambda x: x[1] in ['722511'])\
               .map(lambda x:x[0])\
               .collect())
    bigbox_list=set(coreid.filter(lambda x: x[1]=='452210' or x[1]=='452311')\
               .map(lambda x:x[0])\
               .collect())
    cstore_list=set(coreid.filter(lambda x: x[1]=='445120')\
               .map(lambda x:x[0])\
               .collect())
    drink_list=set(coreid.filter(lambda x: x[1]=='722410')\
               .map(lambda x:x[0])\
               .collect())
    lrest_list=set(coreid.filter(lambda x: x[1]=='722513')\
               .map(lambda x:x[0])\
               .collect())
    pnd_list=set(coreid.filter(lambda x: x[1]=='446110' or x[1]=='446191')\
               .map(lambda x:x[0])\
               .collect())
    snb_list=set(coreid.filter(lambda x: x[1]=='311811' or x[1]=='722515')\
               .map(lambda x:x[0])\
               .collect())
    sfs_list=set(coreid.filter(lambda x: x[1] in ['445210', '445220', '445230', '445291', '445292','445299'])\
               .map(lambda x:x[0])\
               .collect())
    superm_list=set(coreid.filter(lambda x: x[1]=='445110')\
               .map(lambda x:x[0])\
               .collect())

    data0=sc.textFile(nyc_rest)\
      .map(lambda x: next(csv.reader([x])))\
      .map(lambda x: (x[1],x[12],x[13], x[16]))\
      .cache()

    rest_data=data0.filter(lambda x: x[0] in rest_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list) \
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    bigbox_data=data0.filter(lambda x: x[0] in bigbox_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list) \
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    cstore_data=data0.filter(lambda x: x[0] in cstore_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list) \
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    drink_data=data0.filter(lambda x: x[0] in drink_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list) \
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    lrest_data=data0.filter(lambda x: x[0] in lrest_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list) \
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    pnd_data=data0.filter(lambda x: x[0] in pnd_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()

    snb_data=data0.filter(lambda x: x[0] in snb_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    sfs_data=data0.filter(lambda x: x[0] in sfs_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()
    superm_data=data0.filter(lambda x: x[0] in superm_list)\
                    .map(geteveryday)\
                    .flatMap(combine)\
                    .groupByKey()\
                    .mapValues(list)\
                    .map(lambda x: (x[0][:4],datetime.datetime.strptime(x[0],"%Y-%m-%d").date(), int((median(x[1])+std(x[1]))), int((median(x[1])-std(x[1]))), int(median(x[1]))))\
                    .map(lambda x:  x if x[0]=='2020' else (x[0], x[1].replace(year=2020), x[2],x[3],x[4]))\
                    .cache()


    df_rest1 = spark.createDataFrame(rest_data, ['year', 'date','high','low','median'])
    df_rest = df_rest1.sort('year', 'date')

    df_bigbox1 = spark.createDataFrame(bigbox_data, ['year', 'date','high','low','median'])
    df_bigbox = df_bigbox1.sort('year', 'date')

    df_cstore1 = spark.createDataFrame(cstore_data, ['year', 'date','high','low','median'])
    df_cstore = df_cstore1.sort('year', 'date')

    df_drink1 = spark.createDataFrame(drink_data, ['year', 'date','high','low','median'])
    df_drink= df_drink1.sort('year', 'date')

    df_lrest1 = spark.createDataFrame(lrest_data, ['year', 'date','high','low','median'])
    df_lrest= df_lrest1.sort('year', 'date')

    df_pnd1 = spark.createDataFrame(pnd_data, ['year', 'date','high','low','median'])
    df_pnd= df_pnd1.sort('year', 'date')

    df_snb1 = spark.createDataFrame(snb_data, ['year', 'date','high','low','median'])
    df_snb= df_snb1.sort('year', 'date')

    df_sfs1 = spark.createDataFrame(sfs_data, ['year', 'date','high','low','median'])
    df_sfs= df_sfs1.sort('year', 'date')

    df_superm1 = spark.createDataFrame(superm_data, ['year', 'date','high','low','median'])
    df_superm= df_superm1.sort('year', 'date')

    df_rest.coalesce(1).write.option("header", "true").csv('full_service_restaurants.csv')
    df_bigbox.coalesce(1).write.option("header", "true").csv('big_box_grocers.csv')
    df_cstore.coalesce(1).write.option("header", "true").csv('convenience_stores.csv')
    df_drink.coalesce(1).write.option("header", "true").csv('drinking_places.csv')
    df_lrest.coalesce(1).write.option("header", "true").csv('limited_service_restaurants.csv')
    df_pnd.coalesce(1).write.option("header", "true").csv('pharmacies_and_drug_stores.csv')
    df_snb.coalesce(1).write.option("header", "true").csv('snack_and_bakeries.csv')
    df_sfs.coalesce(1).write.option("header", "true").csv('specialty_food_stores.csv')
    df_superm.coalesce(1).write.option("header", "true").csv('supermarkets_except_convenience_stores.csv')