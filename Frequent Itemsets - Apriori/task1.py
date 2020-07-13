# -*- coding: utf-8 -*-
''' 
Author - Anish Amul Vaidya
Task-1
Case 1 - frequent businesses
Case 2 - frequent users
'''

import time
import csv
import sys
from pyspark import SparkConf, SparkContext
from itertools import combinations

case_number = 1
support = 4
input_path = 'data/small2.csv'
output_path = 'task1_op.txt'

def write_output(frequent_itemsets, output_path):
    output_file = open(output_path, 'a')
    
    for i in range(len(frequent_itemsets) -1):
        if isinstance(frequent_itemsets[i], str):
            if isinstance(frequent_itemsets[i + 1], str):
                output_file.write("('" + frequent_itemsets[i] + "'),")
            else:
                output_file.write("('" + frequent_itemsets[i] + "')\n\n")
        if isinstance(frequent_itemsets[i], tuple):
            if len(frequent_itemsets[i + 1]) > len(frequent_itemsets[i]):
                add_comma = False
            else:
                add_comma = True
            if (isinstance(frequent_itemsets[i], tuple) and add_comma):
                output_file.write(str(frequent_itemsets[i]) + ",")
            else:
                output_file.write(str(frequent_itemsets[i]) + "\n\n")
            # if len(frequent_itemsets[i + 1]) > len(frequent_itemsets[i]):
            #     output_file.write("\n\n")
            # if isinstance(frequent_itemsets[i + 1], tuple):
            #     output_file.write(str(frequent_itemsets[i + 1]) + ",")
    output_file.write(str(frequent_itemsets[len(frequent_itemsets) - 1]) + "\n\n")      
    output_file.close()
            


def apriori(data_chunk, support):
    baskets = list(data_chunk)
    count_items = {}
    frequent_itemsets = list()
    for basket in baskets:
        items = basket[1]
        for item in items:
            if item in count_items:
                count_items[item] += 1
            else:
                count_items[item] = 1
    frequent_singles = set()
    
    for item in count_items:
        if count_items[item] >= support:
            frequent_singles.add(item)
    # add singles to frequent itemsets
    frequent_singles = sorted(frequent_singles)
    frequent_itemsets.extend(frequent_singles)
    n_items = 2
    new_added = True
    new_itemsets = list()
    while (new_added):
        if n_items > 2:
            frequent_singles = set()
            for candidate in new_itemsets:
                for item in candidate:
                    frequent_singles.add(item)
        new_added = False
        frequent_singles = list(frequent_singles)
        frequent_singles = sorted(frequent_singles)
        candidates = list(combinations(frequent_singles, n_items))
        for candidate in candidates:
            # print (candidate, " selected")
            candidate_count = 0
            for basket in baskets:
                # print (basket, " basket")
                is_present = True
                for item in candidate:
                    if item not in basket[1]:
                        is_present = False
                        break
                if is_present:
                    candidate_count += 1
            if candidate_count >= support:
                
                new_itemsets.append(candidate)
                frequent_itemsets.append(candidate)
                new_added = True
        n_items += 1
    
    return frequent_itemsets



start = time.time()
conf = SparkConf().setAppName("Task-2")
sc = SparkContext(conf=conf)
input_file = sc.textFile(input_path).mapPartitions(lambda x: csv.reader(x))
headers = input_file.first()
# removing headers user_id, business_id
input_file = input_file.filter(lambda x: x != headers)

# create tuple (pair) RDD
if case_number == 1:
    input_file = input_file.map(lambda x: (x[0], x[1]))
elif case_number == 2:
    input_file = input_file.map(lambda x: (x[1], x[0]))
# creating baskets
big_baskets = input_file.groupByKey().map(lambda x: (x[0], set(x[1]))).cache()
big_baskets = big_baskets.partitionBy(1)
big_baskets = big_baskets.mapPartitions(lambda x: apriori(x, support))

frequent_itemsets = big_baskets.collect()
sc.stop()
output_file = open(output_path, 'w')
output_file.close()


# output_file = open(output_path, 'a')
# output_file.write("Candidates:\n")
# output_file.close()
# write_output(frequent_itemsets, output_path)
output_file = open(output_path, 'a')
output_file.write("Frequent Itemsets:\n")
output_file.close()
write_output(frequent_itemsets, output_path)
print("\nDuration:", str(time.time() - start))