# -*- coding: utf-8 -*-
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import json
import time
from operator import add

review_file = 'data/review.json'
output_file = 'task3.json'
partition_type = "default"  #"default"
numPartitions = 27
threshold = 10

'''
review_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = sys.argv[3]
numPartitions = sys.argv[4]
threshold = sys.argv[5]
'''

def write_output(numPartitions, n_items, business_review_filtered):
    answer = {}
    answer["n_partitions"] = numPartitions
    answer["n_items"] = n_items
    outerlist = []
    for row in business_review_filtered:
        inner_list = []
        inner_list.append(row[0])
        inner_list.append(row[1])
        outerlist.append(inner_list)
    answer["result"] = outerlist
    with open(output_file, 'w+') as fp:
        json.dump(answer, fp)

def partition_by_key(x):
    return sum(ord(ch) for ch in x[:3]) % numPartitions

def items_per_partition(index, i):
    count = 0
    for _ in i:
        count = count + 1
    return index, count



# review_data = sc.textFile(review_file)
# review_rdd = review_data.map(json.loads).map(lambda row: (row["business_id"], 1)).cache()

def vanilla_partition():
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    review_data = sc.textFile(review_file)
    review_rdd = review_data.map(json.loads).map(lambda row: (row["business_id"], 1)).persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    n_items = review_rdd.mapPartitionsWithIndex(items_per_partition).collect()
    n_items = n_items[1::2]
    numPartitions = review_rdd.getNumPartitions()
    # business_review_rdd = review_rdd.reduceByKey(lambda x,y : x + y)
    business_review_rdd = review_rdd.aggregateByKey(0, lambda x,y : x+ y, lambda x, y : x + y)
    business_review_filtered = business_review_rdd.filter(lambda x: x[1] > threshold).collect()
    write_output(numPartitions, n_items, business_review_filtered)
    sc.stop()
    
def custom_partition():
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    review_data = sc.textFile(review_file)
    review_rdd = review_data.map(json.loads).map(lambda row: (row["business_id"], 1)).persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    business_review_rdd = review_rdd.partitionBy(numPartitions, partition_by_key)
    # business_review_rdd = review_rdd.repartition(numPartitions)
    # business_review_rdd = review_rdd.repartitionAndSortWithinPartitions(numPartitions, partition_by_key).persist()
    
    n_items = business_review_rdd.mapPartitionsWithIndex(items_per_partition).collect()
    n_items = n_items[1::2]
    
    # n_items = business_review_rdd.glom().map(len).collect()
    
    business_review_rdd = business_review_rdd.reduceByKey(lambda x,y : x + y, numPartitions, partitionFunc = partition_by_key)
    business_review_filtered = business_review_rdd.filter(lambda x: x[1] > threshold).collect()
    write_output(numPartitions, n_items, business_review_filtered)
    sc.stop()
    
if __name__ == "__main__":
    # conf = SparkConf().setAppName("app")
    
    
    
    if partition_type == "default":
        start = time.time()
        vanilla_partition()
        print ("\nVanilla takes", str(time.time() - start))
    elif partition_type == "customized":
        start = time.time()
        custom_partition()
        print ("\nCustomized takes", str(time.time() - start))
    
    # start = time.time()
    # vanilla_partition()
    # print ("\nVanilla takes", str(time.time() - start))
    # start = time.time()
    # custom_partition()
    # print ("\nCustomized takes", str(time.time() - start))
    
    
    
    
    
    # sc.stop()