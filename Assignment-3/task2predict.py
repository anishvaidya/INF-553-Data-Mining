#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 16:13:08 2020

@author: vanish
"""
#%%
import pickle
import json
import time
import sys
import math
from pyspark import SparkConf, SparkContext

#%%
def write_output(cosine_similarity_list: list) -> None:
   with open(output_file, "w+") as file:
       for pair in cosine_similarity_list:
         # file.write(pair[0][0] + "," + pair[0][1] + "," + str(pair[1]) + "\n")
         row_dict = {}
         row_dict["user_id"] = pair[1]
         row_dict["business_id"] = pair[0]
         row_dict["sim"] = pair[2]
         json.dump(row_dict, file)
         file.write("\n")
   file.close()

#%%
def load_model(model_file: str):
    with open(model_file, 'rb') as model:
        return pickle.load(model)

#%%
def cosine_similarity(business_id: str, user_id: str) -> float:
    try:
        business_vector = set(business_profile[business_id])
        user_vector = set(user_profile[user_id])
    except KeyError:
        return float(0)
    dot_product = len(business_vector.intersection(user_vector))
    len_business_vector = float(math.sqrt(len(business_vector)))
    len_user_vector = float(math.sqrt(len(user_vector)))
    if len_business_vector != 0 and len_user_vector != 0:
        cosine_similarity = float(float(dot_product) / float(len_business_vector * len_user_vector))
        return cosine_similarity
    else:
        return float(0)
    
    '''try:
        profile1 = business_profile[business_id]
        profile2 = user_profile[user_id]
        if len(profile1) != 0 and len(profile2) != 0:
            profile1 = set(profile1)
            profile2 = set(profile2)
            numerator = len(profile1.intersection(profile2))
            denominator = math.sqrt(len(profile1)) * math.sqrt(len(profile2))
            return numerator / denominator
        else:
            return 0.0
    except KeyError:
        return 0.0'''

#%%
test_file = "data/test_review.json"
model_file = "task2.model"
# model_file = "pratu_task2.model"
output_file = "task2.predict"

#%%
start = time.time()
conf = SparkConf().setAppName("Task-2").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#%%
business_profile, user_profile = load_model(model_file)
test_rdd = sc.textFile(test_file, 20)
test_rdd = test_rdd.map(json.loads).map(lambda row: (row["business_id"], row["user_id"])).cache()

data = test_rdd.count()
prediction_list = test_rdd.map(lambda x: (x[0], x[1], cosine_similarity(x[0], x[1]))).filter(lambda x: x[2] >= 0.01).collect()
write_output(prediction_list)

sc.stop()
print ("\nDuration:" + str(time.time() - start))