#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 26 20:28:57 2020

@author: vanish
"""

#%%
import time
import sys
from pyspark import SparkConf, SparkContext, SQLContext
import json
import binascii

#%%
def write_output(predictions: list) -> None:
    with open(output_file, "w+") as file:
        file.write((' '.join(map(str, predictions))))
    file.close()
        
#%%
def hash_fn1(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % n_bits


def hash_fn2(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % n_bits
#%%
def generate_n_hashes(city: str) -> list:
    hash_list = list()
    if city == "":
        return hash_list
    x = int(binascii.hexlify(city.encode('utf8')), 16)
    for i in range(1, n_hashes + 1):
        hash_value = ((i * hash_fn1(x) + i * hash_fn2(x) + i * i) % 37307) % n_bits
        hash_list.append(hash_value)
    return hash_list

#%%
def build_bitarray(train_hash_list: list) -> list:
    for hash_list in train_hash_list:
        for index in hash_list[1]:
            bit_array[index] = 1
    return bit_array

#%%
def throw_darts(test_hash_list: list, bit_array: list) -> list:
    # check if all indexes have 1 in bit_array, for every single record
    # predictions = [0] * len(test_hash_list)
    predictions = list()
    count = -1
    for row in test_hash_list:
        count += 1
        flag = True
        hash_list = row[1]
        if len(hash_list) == 0:
            predictions.append(0)
        else:
            for index in hash_list:
                if bit_array[index]:
                    continue
                else:
                    flag = False
                    break
            if flag:
                predictions.append(1)
            else:
                predictions.append(0)
    return predictions

#%%
def show_analysis(predictions: list):
    zeros = 0
    ones = 1
    for value in predictions:
        if value == 0:
            zeros += 1
        else:
            ones += 1
    return ones, zeros
        
#%%
train_file_path = "data/business_first.json"
test_file_path = "data/business_second.json"
output_file = "task1.output"
'''
train_file_path = sys.argv[1]
test_file_path = sys.argv[2]
output_file = sys.argv[3]'''

#%%
n_hashes = 32
n_bits = 2 ** 16

bit_array = [0] * n_bits

#%%
start = time.time()
conf = SparkConf().setAppName("Task-1").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlcontext = SQLContext(sc)

#%%
train_rdd = sc.textFile(train_file_path).map(json.loads).map(lambda row: row["city"])
train_city_rdd = train_rdd.filter(lambda city: city != "" or " ").distinct()

train_hash_list = train_city_rdd.map(lambda city: (city, generate_n_hashes(city))).collect()
# build_bitarray_rdd = train_hash_rdd.map(lambda x: (build_bitarray(x)))
bit_array = build_bitarray(train_hash_list)

#%%
test_rdd = sc.textFile(test_file_path).map(json.loads).map(lambda row: row["city"])
test_hash_list = test_rdd.map(lambda city: (city, generate_n_hashes(city))).collect()

predictions = throw_darts(test_hash_list, bit_array)
write_output(predictions)
ones, zeros = show_analysis(predictions)

#%%
sc.stop()
print ("\nDuration:" + str(time.time() - start))


