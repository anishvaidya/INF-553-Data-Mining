#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 13:11:37 2020

@author: vanish
"""

#%%
import time
import sys
import json
from pyspark import SparkConf, SparkContext
from operator import add
import string
import math

#%%
def build_model(business_pearson_list: list) -> None:
   with open(model_file, "w+") as file:
       for pair in business_pearson_list:
         # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
         row_dict = {}
         row_dict["b1"] = pair[0][0]
         row_dict["b2"] = pair[0][1]
         row_dict["sim"] = pair[1]
         json.dump(row_dict, file)
         file.write("\n")
   file.close()

#%%
def weighted_average(ratings_list: list) -> float:
    average_rating = 0
    divisor = 0
    i = 1
    for rating in ratings_list[::-1]:
        average_rating += i * rating
        divisor += i
        i *= 0.8
    return float(float(average_rating) / divisor)
    
#%%
def create_business_ratings_dict(business_ratings_list: list) -> dict:
    business_ratings_dict = dict()
    for rating in business_ratings_list:
        business_ratings_dict[rating[0]] = rating[1]
    return business_ratings_dict

#%%
def filter_dissimilar_pairs(list1: list, list2: list) -> bool:
    len_intersection = len(set(list1).intersection(set(list2)))
    if len_intersection >= min_co_ratings:
        return True
    else:
        return False

#%%
def pearson_correlation(pair: tuple, i_dict: dict, j_dict: dict) -> float:
    U = set(i_dict.keys()).intersection(set(j_dict.keys()))
    len_U = len(U)
    avg_ri = float(float(sum(i_dict[user] for user in U)) / float(len_U))
    avg_rj = float(float(sum(j_dict[user] for user in U)) / float(len_U))
    numerator = 0
    denominator_1 = 0
    denominator_2 = 0
    for user in U:
        numerator += float(float((i_dict[user] - avg_ri)) * float((j_dict[user] - avg_rj)))
        denominator_1 += float(float((i_dict[user] - avg_ri)) ** 2)
        denominator_2 += float(float((j_dict[user] - avg_rj)) ** 2)
    denominator = float(math.sqrt(denominator_1)) * float(math.sqrt(denominator_2))
    try:
        w_ij = float(numerator) / float(denominator)
    except ZeroDivisionError:
        return float(0)
    return w_ij    

#%%
train_file = "data/train_review.json"
model_file = "task3item.model"
cf_type = "item_based"
min_co_ratings = 3

#%%
start = time.time()
conf = SparkConf().setAppName("Task-3-train").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#%%
input_data = sc.textFile(train_file, 4)
input_rdd = input_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"], row["stars"])).cache()

'''
case - item based
item - business
'''

# taking care of multiple reviews by same user - weighted average
business_ratings_rdd = input_rdd.map(lambda x: ((x[0], x[1]), x[2])).groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().distinct().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], create_business_ratings_dict(x[1])))

# multiple reviews not handled
'''business_ratings_rdd = input_rdd.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().distinct().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], create_business_ratings_dict(x[1])))'''

business_ratings_rdd = business_ratings_rdd.filter(lambda x: len(x[1]) >= min_co_ratings)
business_ratings_dict = business_ratings_rdd.collectAsMap()

business_id_rdd = business_ratings_rdd.map(lambda x: x[0])
business_pair_rdd = business_id_rdd.cartesian(business_id_rdd).filter(lambda x: x[0] < x[1]).filter(lambda x: filter_dissimilar_pairs(business_ratings_dict[x[0]].keys(), business_ratings_dict[x[1]].keys()))

business_pearson_rdd = business_pair_rdd.map(lambda pair: (pair, pearson_correlation(pair, business_ratings_dict[pair[0]], business_ratings_dict[pair[1]]))).filter(lambda x: x[1] > 0)
business_pearson_list = sorted(business_pearson_rdd.collect(), key = lambda x: (x[0][0], -x[1]), reverse = False)

build_model(business_pearson_list)
sc.stop()
print ("\nDuration:" + str(time.time() - start))