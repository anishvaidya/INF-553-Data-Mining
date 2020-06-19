#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 19:44:13 2020

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
def generate_neighbourhood(user: str, item: str, hood_size) -> list:
    neighbourhood = list()
    city = set(train_user_business_ratings[user].keys())
    similarity_list = list()
    for item_p in city:
        pair = tuple(sorted([item, item_p]))
        try:
            similarity = model[pair]
        except KeyError:
            similarity = 0
        similarity_list.append((pair, similarity))
        
    neighbourhood = sorted(similarity_list, key = lambda x: x[1])
    return neighbourhood
    
#%%
def predict(user: str, item: str, neighbourhood: list) -> float:
    numerator, denominator = 0.0, 0.0
    for neighbor in neighbourhood:
        pair = tuple(sorted([item, neighbor]))
        numerator += float(train_user_business_ratings[user][neighbor]) * float(model[pair])
        denominator += abs(float(model(pair)))
    prediction = float(numerator / denominator)
    return prediction
    
#%%
train_file = "data/train_review.json"
test_file = "data/test_review.json"
model_file = "task3item.model"
output_file = ""
cf_type = "item_based"

business_avg_file = "data/business_avg.json"

hood_size = 3
#%%
start = time.time()
conf = SparkConf().setAppName("Task-3-train").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

model = sc.textFile(model_file).map(json.loads).map(lambda row: ((row["b1"], row["b2"]), row["sim"])).collectAsMap()

train_data = sc.textFile(train_file)
train_data = train_data.map(json.loads).map(lambda row: ((row["user_id"], row["business_id"]), row["stars"])).cache()
train_user_business_ratings = train_data.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], dict((k, v) for k, v in x[1]))).collectAsMap()


test_file = sc.textFile(test_file)
test_file = test_file.map(json.loads).map(lambda row: (row["business_id"], row["user_id"]))

# x[0] = business, x[1] = user
neighborhood_rdd = test_file.map(lambda x: (x[1], x[0], generate_neighbourhood(x[1], x[0], hood_size))).collect()


sc.stop()
print ("\nDuration:" + str(time.time() - start))
