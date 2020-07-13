# -*- coding: utf-8 -*-
import sys
import pyspark
from pyspark import SparkConf, SparkContext
import json
import time

#  /opt/spark/bin/spark-submit  task1.py

start = time.time()
conf = SparkConf().setAppName("app")
sc = SparkContext(conf=conf)


input_file = 'data/review.json'
stopwords_file = 'data/stopwords'
output_file = 'op_task1.json'
year = "2017"
top_m_users = 4
top_n_words = 20

##
# input_file = sys.argv[1]
# output_file = sys.argv[2]
# stopwords_file = sys.argv[3]

# year = sys.argv[4]
# top_m_users = int(sys.argv[5])
# top_n_words = int(sys.argv[6])

###



input_data = sc.textFile(input_file)
rdd = input_data.map(json.loads).map(lambda row:(row['user_id'],row['text'], row['date'])).cache()
answer = {}


# total count
answer["A"] = rdd.count()

# review count per year
answer["B"] = rdd.map(lambda x: x[2]).filter(lambda x: x[:4] == year).count()

# count of distinct users

distinct_users = rdd.map(lambda x: x[0]).groupBy(lambda x: x)

answer["C"] = distinct_users.count()

# Top m users who have the largest number of reviews and its count 



top_m = distinct_users.mapValues(len).sortBy(lambda x: (-x[1], x[0])).take(top_m_users)

outerlist = []
for row in top_m:
    inner_list = []
    inner_list.append(row[0])
    inner_list.append(row[1])
    outerlist.append(inner_list)

answer["D"] = outerlist


words = rdd.flatMap(lambda x: x[1].lower().split()).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# word_count = words.map(lambda x: (x[1], x[0])).sortByKey(False).take(top_n_words)


stopword_list = sc.textFile(stopwords_file)
stopwords = stopword_list.map(lambda x: x).collect()
extra = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")", " ", ""]
stopwords += extra


words_new = words.filter(lambda x: x[0] not in stopwords)
word_count = words_new.map(lambda x: (x[1], x[0])).sortByKey(False).take(top_n_words)

outerlist = []
for row in word_count:
    outerlist.append(row[1])
answer["E"] = outerlist


with open(output_file, 'w+') as fp:
    json.dump(answer, fp)

print ("\nTask 1 takes ", str(time.time() - start))