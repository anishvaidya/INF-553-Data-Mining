#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 17:21:14 2020

@author: vanish
"""
#%%
import time
import csv
import sys
import json
from pyspark import SparkConf, SparkContext
from itertools import combinations
from operator import add
#%%
# n_hashes = 201
# bands = 25
# rows = n_hashes // 25
n_hashes = 75
bands = 75
rows = n_hashes // bands
#%%
def write_output(j_similarity_list: list):
   with open(output_file, "w+") as file:
       for pair in j_similarity_list:
         # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
         row_dict = {}
         row_dict["b1"] = pair[0][0]
         row_dict["b2"] = pair[0][1]
         row_dict["sim"] = pair[1]
         json.dump(row_dict, file)
         file.write("\n")
   file.close()


#%%
def hash_fn1(x: int) -> int:
    # return x * 37 % 991
    return ((x * 1996 + 21) % 26777) % n_buckets_for_sig
    # a, b, c = 80399797, 30792189999, 15728696767
    # return (x * a + b) % c

def hash_fn2(x: int) -> int:
    # return x * 41 % 967
    return ((x * 1996 + 21) % 26777) % n_buckets_for_sig
    # a, b, c = 15432121, 769717171, 393241737373
    # return (x * a + b) % c
    
#%%
def build_user_dict(user_set: set) -> dict:
    user_dict = {}
    index = 0
    for user in user_set:
        user_dict[user] = index
        index += 1
    return user_dict

def build_matrix_correct(user_reviewed: set, user_dict: dict) -> list:
    column = list()
    for user in user_reviewed:
        column.append(user_dict[user])
    return column

#%%
# Shingling
def build_matrix(users_reviewed: set, user_set: set) -> list:
    column = list()
    index = 0
    for user in user_set:
        if user in users_reviewed:
            column.append(index)
        index += 1
    return column

#%%
# Min hashing
def build_signatures(user_list: list, p: int, m: int) -> list:
    signature_list = list()
    for i in range(1, n_hashes + 1):
        # a = [1, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37]
        hash_value = list()
        for user in user_list:
            # hash_value.append(((a[i % len(a)] * hash_fn1(user) + a[i % len(a)] * hash_fn2(user) + a[i % len(a)] * a[i % len(a)]) % p ) % m)
            hash_value.append(((i * hash_fn1(user) + i * hash_fn2(user) + i * i) % p) % m)
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
    # combination_list = combinations(candidate_set, 2)
    
    return combinations(sorted(candidate_set), 2)

#%%
def jaccard_similarity(pair):
    set_A = set(original_matrix_data[pair[0]])
    set_B = set(original_matrix_data[pair[1]])
    n_union = len(set_A.union(set_B))
    n_intersection = len(set_A.intersection(set_B))
    jaccard_similarity = float(float(n_intersection) / float(n_union))
    return jaccard_similarity
#%%
start = time.time()
conf = SparkConf().setAppName("Task-1").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#%%
input_file = 'data/train_review.json'
reqd_jaccard_similarity = 0.05
output_file = 'task1_op'

#%%
input_data = sc.textFile(input_file)
input_rdd = input_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"])).cache()
business_buckets = input_rdd.groupByKey().map(lambda x: (x[0], set(x[1])))#.collect()

users_list = sorted(input_rdd.map(lambda x: x[1]).distinct().collect())
n_buckets_for_sig = len(users_list)
user_set = set(users_list)

#############
# user_index={}
# k=0
# for i in user_set:
#   user_index[i]= k
#   k+=1 

# # matrix1= input_rdd.map(lambda x: (x[0],[user_index[x[1]]])).reduceByKey(add)
# # k1=matrix1.collect()
# # k1 = {item[0] : item[1] for item in k1}
# # original_matrix = matrix1
# # original_matrix = matrix1.sortByKey()
# # original_matrix_data = k1
'''
user_dict = build_user_dict(user_set)
original_matrix = input_rdd.map(lambda x: (x[0], [user_dict[x[1]]])).reduceByKey(add)
original_matrix_data = original_matrix.collect()
original_matrix_data = {item[0] : item[1] for item in original_matrix_data}'''
##
# """this is for testing"""
# user_dict = build_user_dict(user_set)
# original_matrix1 = business_buckets.map(lambda row: (row[0], build_matrix_correct(row[1], user_dict)))
# from operator import add
# original_matrix_data1 = input_rdd.map(lambda x: (x[0], [user_dict[x[1]]])).reduceByKey(add)
# original_matrix_data1 = original_matrix1.collect()
# original_matrix_data2 = {item[0] : item[1] for item in original_matrix_data1}
##

# original_matrix = business_buckets.map(lambda row: (row[0], build_matrix(row[1], user_set))).collect()
original_matrix = business_buckets.map(lambda row: (row[0], build_matrix(row[1], user_set)))
original_matrix_data = original_matrix.collect()
original_matrix_data = {item[0] : item[1] for item in original_matrix_data}
# signature_matrix = original_matrix.map(lambda row: (row[0], sorted(build_signatures(row[1], 27073, n_buckets_for_sig)))).collect()
signature_matrix = original_matrix.map(lambda row: (row[0], build_signatures(row[1], 37307, n_buckets_for_sig))) # 37307

# bands_data = signature_matrix.flatMap(lambda row: build_candidates_from_bands(row)).collect()
bands_data = signature_matrix.flatMap(lambda row: build_candidates_from_bands(row))
bands_data = bands_data.groupByKey().map(lambda row: (row[0], set(row[1]))).filter(lambda row: len(row[1]) > 1)
# bands_data = bands_data.reduceByKey(add).sortByKey().filter(lambda x: len(x[1]) > 1).collect()

candidate_pairs = bands_data.flatMap(lambda row: build_pairs(row[1])).distinct()
c = candidate_pairs.collect()
j_similarity_list = candidate_pairs.map(lambda pair: (pair, jaccard_similarity(pair))).filter(lambda pair: pair[1] >= 0.05).collect()
j_similarity_list = sorted(j_similarity_list)

ans = sorted(j_similarity_list, key = lambda x: x[0][1])
sc.stop()

write_output(j_similarity_list)
print ("\nDuration:" + str(time.time() - start))





    

# test = input_rdd.map(lambda row: row[1]).map(lambda x: (set(x))).collect()

