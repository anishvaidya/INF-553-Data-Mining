# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
''' 
Author - Anish Amul Vaidya
Task-2
'''

import time
import csv
import sys
from pyspark import SparkConf, SparkContext
from itertools import combinations, chain

filter_threshold = 70
support = 50
input_path = 'preprocessed_data.csv'
output_path = 'task2_different_approach_op.txt'

#%%
def prepare_to_write(input_list, case):
    if case == "candidate":
        correct_format_list = []
        for element in input_list:
            correct_format_list.extend(element[1])
    elif case == "frequent":
        correct_format_list = []
        for element in input_list:
            if len(element) == 1:
                correct_format_list.append(element[0])
            else:
                correct_format_list.append(element)
    return correct_format_list

#%%
def write_output(frequent_itemsets, output_path):
    output_file = open(output_path, 'a')
    if len(frequent_itemsets) > 0:
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
        if isinstance(frequent_itemsets[len(frequent_itemsets) - 1], str):
            output_file.write("('" + frequent_itemsets[len(frequent_itemsets) - 1] + "')")
        else:
            output_file.write(str(frequent_itemsets[len(frequent_itemsets) - 1]) + "\n")
    output_file.close()
#%%
def get_combinations(itemset_list, n_items):
    # combinations_list = []

    # for i in range(len(itemset_list) - 1):
    #     for j in range(i + 1, len(itemset_list)):

    #         if itemset_list[i][0:(n_items - 2)] == itemset_list[j][0:(n_items - 2)]:
    #             new_itemset = tuple(set(itemset_list[i]).union(set(itemset_list[j])))
    #             combinations_list.append(tuple(sorted(new_itemset)))
    # return combinations_list
    combinations_list = set()

    for item1 in itemset_list:
        for item2 in itemset_list:
            item_union = set(item1).union(set(item2))
            if len(item_union) == n_items:
                combinations_list.add(tuple(sorted(item_union)))
    return sorted(combinations_list)

#%%    
def apriori(data_chunk, total_data):
    baskets = list()
    count_items = dict()
    chunk_size = 0
    for basket in data_chunk:
        baskets.append(basket)
        chunk_size += 1
        for item in basket:
            if item not in count_items:
                count_items[item] = 1
            else:
                count_items[item] += 1
    
    partition_support = (float(chunk_size) / float(total_data)) * support
    frequent_singles = set()
    for item in count_items:
        if count_items[item] >= partition_support:
            frequent_singles.add(item)
    frequent_singles = sorted(frequent_singles)
    yield (1, frequent_singles)
    
    n_items = 2
    new_added = True
    while new_added:
        new_added = False
        if n_items == 2:
            frequent_itemsets = list()
            candidates_dict = {candidate: 0 for candidate in sorted(list(combinations(frequent_singles, n_items)))}
            
        else:
            candidates_dict = {candidate: 0 for candidate in get_combinations(frequent_itemsets, n_items)}
        for candidate in candidates_dict.keys():
            for basket in baskets:
                ispresent = True
                for item in candidate:
                    if item not in basket:
                        ispresent = False
                        break
                if ispresent:
                    candidates_dict[candidate] += 1
        frequent_itemsets = list()
        for item in candidates_dict:
            if candidates_dict[item] >= partition_support:
                frequent_itemsets.append(item)
        if len(frequent_itemsets) > 0:
            new_added = True
            frequent_itemsets = sorted(frequent_itemsets)
            yield (n_items, frequent_itemsets)
        n_items += 1
#%%
# def apriori(data_chunk, support, n_partitions):
#     partition_support = support / n_partitions
#     baskets = list()
#     count_items = dict()
#     for basket in data_chunk:
#         baskets.append(basket)
#         for item in basket:
#             if item not in count_items:
#                 count_items[item] = 1
#             else:
#                 count_items[item] += 1
    
#     frequent_singles = set()
#     for item in count_items:
#         if count_items[item] >= partition_support:
#             frequent_singles.add(item)
#     frequent_singles = sorted(frequent_singles)
#     yield (1, frequent_singles)
    
#     n_items = 2
#     new_added = True
#     while new_added:
#         new_added = False
#         if n_items == 2:
#             frequent_itemsets = list()
#             frequent_itemsets.extend(frequent_singles)
#         else:
#             frequent_singles = sorted(set(chain(*frequent_itemsets)))
#         candidates_dict = {candidate: 0 for candidate in sorted(list(combinations(frequent_singles, n_items)))}
#         for candidate in candidates_dict.keys():
#             for basket in baskets:
#                 if (set([candidate]).issubset(set(basket))):
#                     candidates_dict[candidate] += 1
#         frequent_itemsets = list()
#         for item in candidates_dict:
#             if candidates_dict[item] >= partition_support:
#                 frequent_itemsets.append(item)
#         if len(frequent_itemsets) > 0:
#             new_added = True
#             frequent_itemsets = sorted(frequent_itemsets)
#             yield (n_items, frequent_itemsets)
#         n_items += 1
  #%%      
def count_occurences(data_chunk, candidates):
    true_count = dict()
    for basket in data_chunk:
        for candidate in candidates:
            for candidate_itemset in candidate[1]:
                if isinstance(candidate_itemset, str) and candidate_itemset in basket:
                    if tuple([candidate_itemset]) in true_count:
                        true_count[tuple([candidate_itemset])] += 1
                    else:
                        true_count[tuple([candidate_itemset])] = 1
                elif isinstance(candidate_itemset, tuple):
                    if all([item in basket for item in candidate_itemset]):
                        if candidate_itemset in true_count:
                            true_count[candidate_itemset] += 1
                        else:
                            true_count[candidate_itemset] = 1
                        
    for candidate_itemset in true_count:
        yield candidate_itemset, true_count[candidate_itemset]    
#%%
# def count_occurences(data_chunk, candidates):
#     true_count = dict()
#     for basket in data_chunk:
#         for candidate in candidates:
#             for candidate_itemset in candidate[1]:
#                 if isinstance(candidate_itemset, str) and set(candidate_itemset).issubset(set(basket)):
#                     if tuple([candidate_itemset]) in true_count:
#                         true_count[tuple([candidate_itemset])] += 1
#                     else:
#                         true_count[tuple([candidate_itemset])] = 1
#                 elif isinstance(candidate_itemset, tuple):
#                     if set(candidate_itemset).issubset(set(basket)):
#                         if candidate_itemset in true_count:
#                             true_count[candidate_itemset] += 1
#                         else:
#                             true_count[candidate_itemset] = 1
                        
#     for candidate_itemset in true_count:
#         yield candidate_itemset, true_count[candidate_itemset]    
        
#%%
start = time.time()
output_file = open(output_path, 'w')
output_file.close()     
conf = SparkConf().setAppName("Task-1")
sc = SparkContext(conf=conf)
input_file = sc.textFile(input_path).mapPartitions(lambda x: csv.reader(x))
headers = input_file.first()
# removing headers user_id, business_id
input_file = input_file.filter(lambda x: x != headers)
input_file = input_file.map(lambda x: (x[0], x[1]))
big_baskets = input_file.groupByKey().map(lambda x: (x[0], set(x[1]))).map(lambda x: x[1])
big_baskets = big_baskets.filter(lambda x: len(x) > filter_threshold)
total_data = len(big_baskets.collect())
n_partitions = big_baskets.getNumPartitions()

phase_1 = big_baskets.mapPartitions(lambda x: apriori(x, total_data))

phase_1 = phase_1.reduceByKey(lambda x, y: sorted(set(x + y)))
candidates = phase_1.collect()
phase_2 = big_baskets.mapPartitions(lambda x: count_occurences(x, candidates))
phase_2 = phase_2.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0])

frequent_itemsets = phase_2.collect()

frequent_itemsets = sorted(frequent_itemsets, key = (lambda itemset: (len(itemset), itemset)))

candidates = sorted(candidates, key = (lambda x: x[0]))
candidates = prepare_to_write(candidates, "candidate")
frequent_itemsets = prepare_to_write(frequent_itemsets, "frequent")
#%%
output_file = open(output_path, 'a')
output_file.write("Candidates:\n")
output_file.close()
write_output(candidates, output_path)
output_file = open(output_path, 'a')
output_file.write("\n")
output_file.close()
output_file = open(output_path, 'a')
output_file.write("Frequent Itemsets:\n")
output_file.close()
write_output(frequent_itemsets, output_path)
sc.stop()

print("\nDuration:", str(time.time() - start))
