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
from itertools import combinations

#%%
# =============================================================================
# def build_model(pearson_list: list) -> None:
#    with open(model_file, "w+") as file:
#        if cf_type == "item_based":
#            for pair in pearson_list:
#              # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
#              row_dict = {}
#              row_dict["b1"] = pair[0][0]
#              row_dict["b2"] = pair[0][1]
#              row_dict["sim"] = pair[1]
#              json.dump(row_dict, file)
#              file.write("\n")
#        elif cf_type == "user_based":
#            for pair in pearson_list:
#              # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
#              row_dict = {}
#              row_dict["u1"] = pair[0][0]
#              row_dict["u2"] = pair[0][1]
#              row_dict["sim"] = pair[1]
#              json.dump(row_dict, file)
#              file.write("\n")
#    file.close()
# =============================================================================
   
#%%
def build_model(pearson_list: list) -> None:
   with open(model_file, "w+") as file:
       if cf_type == "item_based":
           for pair in pearson_list:
               row_dict = {}
               row_dict["b1"] = pair[0][0]
               row_dict["b2"] = pair[0][1]
               row_dict["sim"] = pair[1]
               json.dump(row_dict, file)
               file.write("\n")
       elif cf_type == "user_based":
           for pair in pearson_list:
               row_dict = {}
               row_dict["u1"] = pair[0][0]
               row_dict["u2"] = pair[0][1]
               row_dict["sim"] = pair[1]
               json.dump(row_dict, file)
               file.write("\n")
             
   file.close()
      
#%%
def hash_fn1(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % n_buckets_for_sig


def hash_fn2(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % n_buckets_for_sig

#%%
# Shingling
def build_matrix(business_reviewed: dict, business_set: set) -> list:
    column = list()
    index = 0
    business_reviewed_set = set(business_reviewed.keys())
    for business in business_set:
        if business in business_reviewed_set:
            column.append(index)
        index += 1
    return column

#%%
# Min hashing
def build_signatures(business_list: list, p: int, m: int) -> list:
    signature_list = list()
    for i in range(1, n_hashes + 1):
        hash_value = list()
        for business in business_list:
            hash_value.append(((i * hash_fn1(business) + i * hash_fn2(business) + i * i) % p) % m)
        signature_list.append(min(hash_value))
    return signature_list  

#%%  
def build_candidates_from_bands(signature: tuple) -> list:
    bucket_of_bands = list()
    for i in range(bands):
        row_data = signature[1][(rows * i): (rows * (i + 1))]
        bucket_of_bands.append(((i, tuple(row_data)), signature[0]))
        # bucket_of_bands.append(((i, tuple(row_data)), [signature[0]]))
    return bucket_of_bands

#%%
def build_pairs(candidate_set: set) -> list:
    return combinations(sorted(candidate_set), 2)

#%%
def jaccard_similarity(pair: tuple) -> float:
    set_A = set(user_matrix_data[pair[0]])
    set_B = set(user_matrix_data[pair[1]])
    n_union = len(set_A.union(set_B))
    n_intersection = len(set_A.intersection(set_B))
    jaccard_similarity = float(float(n_intersection) / float(n_union))
    return jaccard_similarity
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
    # return round(w_ij, 10)
    return w_ij

#%%
train_file = "data/train_review.json"
model_file = "task3user.model"
cf_type = "user_based"
min_co_ratings = 3

#%%
reqd_jaccard_similarity = 0.01
n_hashes = 36
bands = 36
rows = n_hashes // bands
#%%
start = time.time()
conf = SparkConf().setAppName("Task-3-train").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

#%%
input_data = sc.textFile(train_file, 4)
input_rdd = input_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"], row["stars"])).cache()

'''
case - item based
item - business
'''
if cf_type == "item_based":
    # taking care of multiple reviews by same user - weighted average
    business_ratings_rdd = input_rdd.map(lambda x: ((x[0], x[1]), x[2])).groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().distinct().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], create_business_ratings_dict(x[1])))
    
    # multiple reviews not handled
    ''''business_ratings_rdd = input_rdd.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().distinct().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], create_business_ratings_dict(x[1])))'''
    
    business_ratings_rdd = business_ratings_rdd.filter(lambda x: len(x[1]) >= min_co_ratings)
    business_ratings_dict = business_ratings_rdd.collectAsMap()
    
    business_id_rdd = business_ratings_rdd.map(lambda x: x[0])
    business_pair_rdd = business_id_rdd.cartesian(business_id_rdd).filter(lambda x: x[0] < x[1]).filter(lambda x: filter_dissimilar_pairs(business_ratings_dict[x[0]].keys(), business_ratings_dict[x[1]].keys()))
    
    business_pearson_rdd = business_pair_rdd.map(lambda pair: (pair, pearson_correlation(pair, business_ratings_dict[pair[0]], business_ratings_dict[pair[1]]))).filter(lambda x: x[1] > 0)
    business_pearson_list = sorted(business_pearson_rdd.collect(), key = lambda x: (x[0][0], -x[1]), reverse = False)
    
    build_model(business_pearson_list)
 
elif cf_type == "user_based":
    print ("\nImplement it bro!!!")
    user_rdd = input_rdd.map(lambda row: ((row[1], row[0]), row[2])).cache()
    user_master_rdd = user_rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: (x[0][0], (x[0][1], weighted_average(x[1])))).groupByKey().map(lambda x: (x[0], set(x[1]))).map(lambda x: (x[0], dict((k, v) for k, v in x[1])))
    business_set = set(sorted(input_rdd.map(lambda x: x[0]).distinct().collect()))
    user_ratings_dict = user_master_rdd.collectAsMap()
    n_buckets_for_sig = len(business_set)
    
    user_matrix = user_master_rdd.map(lambda x: (x[0], build_matrix(x[1], business_set)))
    user_matrix_data = user_matrix.collectAsMap()
    user_signature_matrix = user_matrix.map(lambda x: (x[0], build_signatures(x[1], 37307, n_buckets_for_sig)))
    
    bands_data = user_signature_matrix.flatMap(lambda row: build_candidates_from_bands(row))
    bands_data = bands_data.groupByKey().map(lambda row: (row[0], set(row[1]))).filter(lambda row: len(row[1]) > 1)
    candidate_pairs = bands_data.flatMap(lambda row: build_pairs(row[1])).distinct()
    candidate_pairs = candidate_pairs.filter(lambda x: filter_dissimilar_pairs(user_matrix_data[x[0]], user_matrix_data[x[1]]))
    j_similarity_rdd = candidate_pairs.map(lambda pair: (pair, jaccard_similarity(pair))).filter(lambda pair: pair[1] >= reqd_jaccard_similarity).map(lambda x: x[0])
    # .map(lambda x: x[0]).filter(lambda x: filter_dissimilar_pairs(user_matrix_data[x[0]], user_matrix_data[x[1]]))
    # j_similarity_list = sorted(j_similarity_list)
    user_pearson_rdd = j_similarity_rdd.map(lambda pair: (pair, pearson_correlation(pair, user_ratings_dict[pair[0]], user_ratings_dict[pair[1]]))).filter(lambda x: x[1] > 0)
    user_pearson_list = sorted(user_pearson_rdd.collect(), key = lambda x: (x[0][0], -x[1]), reverse = False)
    
    build_model(user_pearson_list)
    
    
    
sc.stop()
print ("\nDuration:" + str(time.time() - start))