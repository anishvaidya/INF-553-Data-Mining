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
def build_model(predictions_rdd: list) -> None:
   with open(output_file, "w+") as file:
       for pair in predictions_rdd:
         # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
         row_dict = {}
         row_dict["user_id"] = pair[0][0]
         row_dict["business_id"] = pair[0][1]
         row_dict["stars"] = pair[1]
         json.dump(row_dict, file)
         file.write("\n")
   file.close()
   
#%%
def weighted_average(ratings_list: list) -> float:
    # average_rating = 0
    # divisor = 0
    # i = 1
    # for rating in ratings_list[::-1]:
    #     average_rating += i * rating
    #     divisor += i
    #     i *= 0.3
    # return float(float(average_rating) / divisor)
    
    return ratings_list[-1]

#%%
def generate_neighbourhood(user: str, item: str, hood_size) -> list:
    neighbourhood = list()
    city = set(train_user_business_ratings[user].keys())
    similarity_list = list()
    for item_p in city:
        pair = tuple(sorted([item, item_p]))
        try:
            similarity = model[pair]
        except KeyError:            # new item
            similarity = 0
        similarity_list.append((item_p, similarity))
        
    neighbourhood = sorted(similarity_list, key = lambda x: x[1], reverse = True)
    
    return neighbourhood[:hood_size]
    
#%%
def predict(user: str, item: str, neighbourhood: list) -> float:
    numerator, denominator = 0.0, 0.0
    if item in train_user_business_ratings[user].keys():
        return train_user_business_ratings[user][item]
    
    for neighbor in neighbourhood:
        try:
            pair = tuple(sorted([item, neighbor]))
            numerator += float(train_user_business_ratings[user][neighbor]) * float(model[pair])
            denominator += abs(float(model[pair]))
            # prediction = float(numerator / denominator)
            # return prediction
        except KeyError:
            return float(avg_business_rating)
    prediction = float(numerator / denominator)
    return prediction

#%%
def new_predict(user: str, item: str) -> float:
    neighbourhood = list()
    city = set(train_user_business_ratings[user].keys())
    similarity_list = list()
    for item_p in city:
        pair = tuple(sorted([item, item_p]))
        similarity = model.get(pair, 0)
        similarity_list.append((item_p, similarity))
    neighbourhood = sorted(similarity_list, key = lambda x: x[1], reverse = True)[:hood_size]
    hood = [item[0] for item in neighbourhood]
    numerator = 0.0
    denominator = 0.0
    for neighbor in hood:
        pair = tuple(sorted([item, neighbor]))
        numerator += train_user_business_ratings[user][neighbor] * model.get(pair, 0)
        denominator += abs(model.get(pair, 0))
        '''
        wij = model.get(pair, 0) * (abs(model.get(pair, 0)) ** 1.5)
        numerator += float(train_user_business_ratings[user][neighbor] * wij)
        denominator += abs(float(wij))'''
    try:
        prediction = numerator / denominator
    except ZeroDivisionError:
        # return avg_business_rating
        return 0
    return prediction
    
#%%
def calculate_rmse(predictions_dict: dict) -> float:
    
    sq_error = 0
    
    n_ratings = len(predictions_list)
    for pair in predictions_list:
        pred_rating = pair[1]
        actual_rating = actual_ratings[pair[0][0]][pair[0][1]]
        sq_error += float(pred_rating - actual_rating) ** 2
    rmse = math.sqrt((sq_error) / n_ratings)
    return rmse
    '''
    count = 0
    for user in actual_ratings.keys():
        for item in actual_ratings[user].keys():
            count += 1
            actual_rating = actual_ratings[user][item]
            try:
                pair = tuple(sorted([user, item]))
                pred_rating = predictions_dict[pair]
            except KeyError:
                pred_rating = avg_business_rating
            sq_error += float(pred_rating - actual_rating) ** 2
    rmse = math.sqrt((sq_error) / count)
    return rmse'''
    
#%%
train_file = "data/train_review.json"
test_file = "data/test_review.json"
test_ratings_file = "data/test_review_ratings.json"
model_file = "task3item.model"
output_file = "task3item.predict"
cf_type = "item_based"

business_avg_file = "data/business_avg.json"
hood_size = 7
avg_business_rating = 3.823989
#%%
start = time.time()
conf = SparkConf().setAppName("Task-3-predict").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

if cf_type == "item_based":
    model = sc.textFile(model_file).map(json.loads).map(lambda row: ((row["b1"], row["b2"]), row["sim"])).collectAsMap()
    
    train_data = sc.textFile(train_file)
    train_data = train_data.map(json.loads).map(lambda row: ((row["user_id"], row["business_id"]), row["stars"])).cache()
    train_user_business_ratings = train_data.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], dict((k, v) for k, v in x[1]))).collectAsMap()
    
    
    test_file = sc.textFile(test_file)
    test_file = test_file.map(json.loads).map(lambda row: (row["business_id"], row["user_id"]))
    
    # x[0] = business, x[1] = user
    '''neighborhood_rdd = test_file.map(lambda x: (x[1], x[0], generate_neighbourhood(x[1], x[0], hood_size))).map(lambda x: (x[0], x[1], [item_p[0] for item_p in x[2]]))
    predictions_rdd = neighborhood_rdd.map(lambda x: ((x[0], x[1]), (predict(x[0], x[1], x[2])))).collect()'''
    
    predictions_rdd = test_file.map(lambda x: ((x[1], x[0]), new_predict(x[1], x[0]))).filter(lambda x: x[1] > 0)
    predictions_list = predictions_rdd.collect()
    # predictions_dict = predictions_rdd.collectAsMap()
    
    # testing rmse
    actual_ratings = sc.textFile(test_ratings_file).map(json.loads).map(lambda row: ((row["user_id"], row["business_id"]), row["stars"]))
    actual_ratings = actual_ratings.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], dict((k, v) for k, v in x[1]))).collectAsMap()
    
    # print (str(calculate_rmse(predictions_dict)))
    build_model(predictions_list)
    sc.stop()

elif cf_type == "user_based":
    print ("\n Implement this bro")
    
print ("\nDuration:" + str(time.time() - start))
