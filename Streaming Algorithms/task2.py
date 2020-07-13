#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 27 14:54:50 2020

@author: vanish
"""

'''java -cp generate_stream.jar StreamSimulation business.json 6633 100'''

#%%
import time
import sys
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.streaming import StreamingContext
import json
import binascii
from statistics import median

#%%
def write_output(x: str):
    with open(output_file_path, "a") as file:
        # file.write(str(x.collect()))
        file.write(str(x) + "\n")

#%%
def find_trailing_zeros(hash_list_master: list) -> list:
    trailing_zeros_master = list()
    for hash_list in hash_list_master:
        trailing_zeros = list()
        for value in hash_list:
            value = bin(value)[2:]
            count = 0
            for bit in value[::-1]:
                if bit == "0":
                    count += 1
                else:
                    break
            trailing_zeros.append(count)
        trailing_zeros_master.append(trailing_zeros)
    return trailing_zeros_master
            
#%%
def hash_fn1(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % (port_number ** 2)

def hash_fn2(x: int) -> int:
    return ((x * 1997 + 15) % 37307) % (port_number ** 2)

#%%
def generate_n_hashes(city: str, n_hashes: int) -> list:
    hash_values = list()
    if city == "" or city == " ":
        # print ("empty")
        return hash_values
    x = int(binascii.hexlify(city.encode('utf8')), 16)
    for i in range(1, n_hashes + 1):
        # print ("non empty")
        hash_value = ((i * hash_fn1(x) + i * hash_fn2(x) + i * i) % 37307) % (port_number ** 2)
        hash_values.append(hash_value)
    return hash_values
           
#%%
def flajolet_martin(stream: str) -> None:
    global first_time
    if first_time:
        first_time = False
        with open(output_file_path, "w+") as output_file:
            output_file.write("Time,Ground Truth,Estimation" + "\n")
    cities = stream.collect()
    # ground_truth = len(cities)
    ground_truth = len(set(cities))
    hash_list_master = list()
    trailing_zeros = list()
    for city in cities:
        hash_list_master.append(generate_n_hashes(city, n_hashes))
    trailing_zeros_master = find_trailing_zeros(hash_list_master)
    # write_output(str(cities))
    max_zeros = [0] * n_hashes
    for bit in range(n_hashes):
        for trailing_zeros in trailing_zeros_master:
            if trailing_zeros[bit] > max_zeros[bit]:
                max_zeros[bit] = trailing_zeros[bit]
    group_average_list = list()
    group_size = n_hashes // 20
    for i in range(0, n_hashes, group_size):
        batch = max_zeros[i: i + group_size]
        batch_avg = sum(batch) / len(batch)
        group_average_list.append(batch_avg)
    median_of_avg = median(group_average_list)
    estimate_count = (2 ** int(median_of_avg))
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    output_line = current_time + "," + str(ground_truth) + "," + str(estimate_count)
    write_output(output_line)
        
#%%
first_time = True
port_number = 6633
output_file_path = "task2.output"

#%%
n_hashes = 100
#%%

start = time.time()
conf = SparkConf().setAppName("Task-1").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
connection = ssc.socketTextStream("localhost", port_number)
stream = connection.window(windowDuration = 30, slideDuration = 10)
data = stream.map(json.loads).map(lambda x:x['city']).foreachRDD(lambda x: flajolet_martin(x))
ssc.start()
ssc.awaitTermination()



