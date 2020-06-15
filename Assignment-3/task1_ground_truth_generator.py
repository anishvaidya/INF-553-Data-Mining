# -*- coding: utf-8 -*-

import time
import csv
import sys
import json
from pyspark import SparkConf, SparkContext

#%%
conf = SparkConf().setAppName("Task-1-ground-truth-generator").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#%%
input_file = 'data/test_review.json'
reqd_jaccard_similarity = 0.05
output_file = 'data/true_similarity_pairs_small.csv'

#%%
input_data = sc.textFile(input_file)
input_rdd = input_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"])).cache()
input_rdd_grouped = input_rdd.groupByKey().map(lambda x: (x[0], set(x[1])))

input_rdd_grouped = input_rdd_grouped.repartition(1)
num_partitions = input_rdd_grouped.getNumPartitions()

business_bucket = input_rdd_grouped.collect()
business_bucket = sorted(business_bucket)



#%%

ans_union = business_bucket[0][1].union(business_bucket[1][1])
ans_intersect = business_bucket[0][1].intersection(business_bucket[1][1])

#%%
n_comparisons = 0
similarity_dict = {}
for i in range(len(business_bucket) - 1):
    for j in range(i + 1, len(business_bucket)):
        n_comparisons += 1
        n_union = len(business_bucket[i][1].union(business_bucket[j][1]))
        n_intersection = len(business_bucket[i][1].intersection(business_bucket[j][1]))
        jaccard_similarity = n_intersection / n_union
        if jaccard_similarity >= reqd_jaccard_similarity:
            similarity_dict[(business_bucket[i][0], business_bucket[j][0])] = jaccard_similarity

#%%

with open(output_file, "w+") as file:
    for key in similarity_dict:
        file.write(key[0] + "," + key[1] + "," + str(similarity_dict[key]) + "\n")
    file.close()
    
#%%    
sc.stop()