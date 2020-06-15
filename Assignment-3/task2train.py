#!/usr/bin/env python3
''''# -*- coding: ISO-8859-1 -*-'''
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 08:57:32 2020

@author: vanish
"""
#%%
import time
import sys
import json
from pyspark import SparkConf, SparkContext
from operator import add
import string
import math
import pickle
import re

#%%
def build_model(output_file: str, business_profile: dict, user_profile: dict) -> None:
    with open(output_file, "wb") as model:
        pickle.dump([business_profile, user_profile], model)
    model.close()

#%%
def get_stopwords(stopwords_file: str) -> set:
    stopwords = set()
    with open(stopwords_file, 'r') as file:
        word = file.readline()
        while word:
            stopwords.add(word.strip())
            word = file.readline()
    return stopwords

#%%
def clean_text(text_list: list, stopwords: set) -> list:
    document = list()
    for text in text_list:
        text = text.translate(str.maketrans('', '', string.punctuation))
        words = text.split()
        for word in words:
            word = word.strip()
            if not(word in stopwords or not(word.isalpha())): # word in stopwords or word.isdigit()
                document.append(word)
    return document
    # for text in text_list:
    #     words = text.split()
    #     for word in words:
    #         word = word.strip()
    #         word = word.translate(str.maketrans('', '', string.punctuation))
    #         if not(word in stopwords or not(word.isalpha())):
    #             document.append(word)
    # return document

#%%
def calculate_word_tf(document: list) -> list:
    word_tf = list()
    word_count = dict()
    max_occurence_count = 0
    for word in document:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
        if word_count[word] > max_occurence_count:
            max_occurence_count = word_count[word]
    for word in word_count:
        if word_count[word] > min_word_count:
            word_tf.append((word, float(float(word_count[word]) / float(max_occurence_count))))
    return sorted(word_tf, key = lambda x: x[1], reverse = True)

#%%
def calculate_word_idf(word_count: int, n_documents: int) -> float:
    return float(math.log((float(n_documents) / float(word_count)), 2))

#%%
def calculate_tf_idf(word_tf_list: list, word_idf_dict: dict) -> list:
    word_tf_idf_list = list()
    for word in word_tf_list:
        word_tf_idf = float(float(word[1]) * float(word_idf_dict[word[0]]))
        word_tf_idf_list.append((word[0],word_tf_idf))
    word_tf_idf_list = sorted(word_tf_idf_list, key = lambda x: x[1], reverse = True)
    if len(word_tf_idf_list) > 200:
        return word_tf_idf_list[:200]
    else:
        return word_tf_idf_list
    
#%%
def assign_word_id(word_dict: dict) -> dict:
    word_id_dict = dict()
    index = 0
    for word in word_dict.keys():
        word_id_dict[word] = index
        index += 1
    return word_id_dict

#%%
def tokenize_words(words_list: list, words_id_dict: dict) -> list:
    tokenized_words_list = list()
    for word in words_list:
        tokenized_words_list.append(words_id_dict[word[0]])
    return tokenized_words_list

#%%
def get_imp_words(tf_idf_list: list) -> list:
    imp_words_list = list()
    for item in tf_idf_list:
        imp_words_list.append(item[0])
    return imp_words_list

#%%
train_file = "data/train_review.json"
model_file = "task2.model"
stopwords_file = "data/stopwords"
min_word_count = 3

#%%
start = time.time()
conf = SparkConf().setAppName("Task-2").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#%%
start = time.time()
input_data = sc.textFile(train_file, 30)
input_rdd = input_data.map(json.loads).map(lambda row: (row["business_id"], row["user_id"], row["text"])).cache()
stopwords = get_stopwords(stopwords_file)

'''
business_rdd = input_rdd.map(lambda row: (row[0], str(row[2]).lower())).groupByKey().map(lambda row: (row[0], list(row[1])))
n_documents_business = business_rdd.count()
business_rdd_documents = business_rdd.map(lambda row: (row[0], clean_text(row[1], stopwords)))
business_tf = business_rdd_documents.map(lambda row: (row[0], calculate_word_tf(row[1])))
business_tf_list = business_tf.collect()

business_idf_dict = business_tf.flatMapValues(lambda row: row).map(lambda row: (row[1][0], 1)).reduceByKey(add).map(lambda x: (x[0], calculate_word_idf(x[1], n_documents_business))).sortByKey().collectAsMap()

business_tf_idf = business_tf.map(lambda row: (row[0], calculate_tf_idf(row[1], business_idf_dict)))
business_imp_words = business_tf_idf.collect()
word_id_dict = assign_word_id(business_idf_dict)
business_imp_words_tokenized = business_tf_idf.map(lambda row: (row[0], tokenize_words(row[1], word_id_dict))).collectAsMap()'''

def train(index: int) -> list:
    business_rdd = input_rdd.map(lambda row: (row[index], str(row[2]).lower())).groupByKey().map(lambda row: (row[0], list(row[1])))
    n_documents_business = business_rdd.count()
    business_rdd_documents = business_rdd.map(lambda row: (row[0], clean_text(row[1], stopwords)))
    business_tf = business_rdd_documents.map(lambda row: (row[0], calculate_word_tf(row[1])))
    # business_tf_list = business_tf.collect()
    
    business_idf_dict = business_tf.flatMapValues(lambda row: row).map(lambda row: (row[1][0], 1)).reduceByKey(add).map(lambda x: (x[0], calculate_word_idf(x[1], n_documents_business))).sortByKey().collectAsMap()
    
    business_tf_idf = business_tf.map(lambda row: (row[0], calculate_tf_idf(row[1], business_idf_dict)))
    business_imp_words = business_tf_idf.map(lambda x: (x[0], get_imp_words(x[1]))).collectAsMap()
    # word_id_dict = assign_word_id(business_idf_dict)
    # business_imp_words_tokenized = business_tf_idf.map(lambda row: (row[0], tokenize_words(row[1], word_id_dict))).collectAsMap()
    return business_imp_words

business_imp_words_tokenized = train(0)
user_imp_words_tokenized = train(1)
    
build_model(model_file, business_imp_words_tokenized, user_imp_words_tokenized)

sc.stop()
print ("\nDuration:" + str(time.time() - start))

