#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 15:57:42 2020

@author: vanish
"""

#%%
import time
import sys
import csv
from pyspark import SparkConf, SparkContext, SQLContext
# from pyspark.sql import SQLContext, SparkSession
from graphframes import GraphFrame
from operator import add

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import os

#%%
def write_output(communities_list: list) -> None:
    with open(community_output_file_path, "w+") as output_file:
        for community in communities_list:
            user_list = community
            for user in user_list[:len(user_list) - 1]:
                output_file.write("\'" + user + "\'" + ", ")
            output_file.write("\'" + user_list[-1] + "\'")
            output_file.write("\n")
    output_file.close()
#%%
def filter_dissimilar_users(user_1: str, user_2: str)-> bool:
    user_1_set = set(user_dict[user_1])
    user_2_set = set(user_dict[user_2])
    len_intersection = len(user_1_set.intersection(user_2_set))
    if len_intersection < filter_threshold:
        return False
    else:
        return True

#%%
filter_threshold = 7
input_file_path = "data/ub_sample_data.csv"
community_output_file_path = "task1.output"

#%%

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

start = time.time()
conf = SparkConf().setAppName("Task-1").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlcontext = SQLContext(sc)
#%%
input_rdd = sc.textFile(input_file_path)
input_rdd = input_rdd.mapPartitions(lambda x: csv.reader(x)) # 1st column - user id, 2nd column - business id
headers = input_rdd.first()
input_rdd = input_rdd.filter(lambda x: x != headers)

#%%
input_rdd = input_rdd.groupByKey().map(lambda x: (x[0], set(x[1])))
user_dict = input_rdd.collectAsMap()
user_initial_rdd = input_rdd.map(lambda x: x[0])
pair_rdd = user_initial_rdd.cartesian(user_initial_rdd).filter(lambda x: x[0] != x[1]).filter(lambda x: filter_dissimilar_users(x[0], x[1]))
user_rdd = pair_rdd.flatMap(lambda x: x).map(lambda x: tuple([x])).distinct()

#%%
users_df = sqlcontext.createDataFrame(user_rdd.collect(), ["id"])
edges_df = sqlcontext.createDataFrame(pair_rdd.collect(), ["src", "dst"])
mygraph = GraphFrame(users_df, edges_df)
communities = mygraph.labelPropagation(maxIter = 5)
mycommunities = communities.collect()

communities_rdd = sc.parallelize(mycommunities).map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], sorted(x[1])))
communities_rdd = communities_rdd.map(lambda x: x[1]).sortBy(lambda x: x[0]).sortBy(lambda x: len(x)).collect()

write_output(communities_rdd)

#%%
sc.stop()
print ("\nDuration:" + str(time.time() - start))
