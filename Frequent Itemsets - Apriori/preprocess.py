# -*- coding: utf-8 -*-
'''
Author - Anish Amul Vaidya
Preprocess JSON
Input - raw JSON
Output - filtered CSV

'''

import sys
import json
from pyspark import SparkConf, SparkContext
import time

def write_to_file(list_to_write, output_path):
    output_file = open(output_path, "w+")
    output_file.write("user_id,business_id\n")
    for line in list_to_write:
        output_file.write(line[0] + "," + line[1] + "\n")
    output_file.close()

start = time.time()
review_file = 'data/review.json'
business_file = 'data/business.json'
output_path = 'preprocessed_data.csv'

conf = SparkConf().setAppName("Task-2")
sc = SparkContext(conf=conf)

review_data = sc.textFile(review_file)
business_data =sc.textFile(business_file)

business_rdd = business_data.map(json.loads).map(lambda row: (row['business_id'], row['state'])).cache()
review_rdd = review_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"])).cache()

joined_rdd = business_rdd.join(review_rdd)
filtered_rdd = joined_rdd.filter(lambda x: x[1][0] == 'NV').map(lambda x: (x[1][1], x[0]))

filtered_list = filtered_rdd.collect()

write_to_file(filtered_list, output_path)
print ("\nDuration:" + str(time.time() - start))
sc.stop()



'''----------------------------------------------------------------------------------------------------------------'''
reviewRDD = sc.textFile(review_file)
businessRDD=sc.textFile(business_file)
business_data= businessRDD.map(json.loads).filter(lambda x:x['state']=="NV").map(lambda x:(x['business_id'],1))
review_data=reviewRDD.map(json.loads).map(lambda x:(x['business_id'],x['user_id']))
results=business_data.join(review_data).map(lambda x:(x[1][1],x[0])).collect()
write_to_file(results, output_path)