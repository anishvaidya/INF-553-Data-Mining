#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 18:41:59 2020

@author: vanish
"""

#%%
import time
import sys
import csv
from pyspark import SparkConf, SparkContext, SQLContext
# from graphframes import GraphFrame
from operator import add
from copy import deepcopy

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import os

#%%
def write_betweenness_output(betweenness_list: list) -> None:
    with open(betweenness_output_file_path, "w+") as output_file:
        for edge in betweenness_list:
            output_file.writelines(str(edge[0]) + ", " + str(edge[1]) + "\n")
    output_file.close()
    
#%%
def write_community_output(communities_list: list) -> None:
    with open(community_output_file_path, "w+") as output_file:
        for community in communities_list:
            user_list = community
            for user in user_list[:len(user_list) - 1]:
                output_file.write("\'" + user + "\'" + ", ")
            output_file.write("\'" + user_list[-1] + "\'")
            output_file.write("\n")
    output_file.close()    
#%%
def build_tree_bfs(vertex: str, vertex_dict: dict) -> dict:
    root = vertex
    parent_dict = dict()
    tree_level = dict()
    diverging_paths = dict()
    queue = list()
    visited = list()
    queue.append(root)
    diverging_paths[root] = 1
    tree_level[root] = 0
    while queue:
        current_node = queue.pop(0)
        visited.append(current_node)
        children = vertex_dict[current_node]
        for child in children:
            if (tree_level.get(child, None) == None):
                queue.append(child)
                tree_level[child] = tree_level[current_node] + 1
                parent_dict[child] = list()
                diverging_paths[child] = 0
            if (tree_level[child] == tree_level[current_node] + 1):
                parent_dict[child].append(current_node)
                diverging_paths[child] += diverging_paths[current_node]
    
    tree_data = dict()
    tree_data["visited"] = visited
    tree_data["parent_dict"] = parent_dict
    tree_data["diverging_paths"] = diverging_paths
    return tree_data

#%%
def remove_edges_update_graph(vertex_dict: dict, betweenness_list: list):
    betweenness_values = list()
    for edge in betweenness_list:
        betweenness_values.append(edge[1])
    max_betweenness = max(betweenness_values)
    edges_to_remove = list()
    for edge in betweenness_list:
        if edge[1] == max_betweenness:
            edges_to_remove.append(edge[0])
    for edge in edges_to_remove:
        vertex_dict[edge[0]].remove(edge[1])
        vertex_dict[edge[1]].remove(edge[0])
    vertex_tree_data = user_rdd.map(lambda x: (x[0], build_tree_bfs(x[0], vertex_dict)))
    
    edge_values_distributed = vertex_tree_data.map(lambda x: (x[0], get_edge_value(x[1]))).collectAsMap()
    betweenness = calculate_betweenness(edge_values_distributed)
    betweenness_list = sorted(betweenness.items(), key = (lambda x: (-x[1], x[0])))

    return vertex_dict, betweenness_list

#%%
def get_modularity(vertex_dict: dict, communities: list, edge_count: int) -> float:
    m = edge_count
    modularity = 0
    for community in communities:
        for i in community:
            for j in community:
                i_set = set(vertex_dict[i])
                k_i = len(vertex_dict[i]) # modularity
                k_j = len(vertex_dict[j])
                if j in i_set:
                    A_ij = 1
                else:
                    A_ij = 0
                modularity += (A_ij - ((k_i * k_j) / (2 * m)))
    modularity = (modularity / (2 * m))
    return modularity
                    
#%%
def generate_communities(vertices_list: list, vertex_dict: dict) -> list:
    remaining_vertices = set(deepcopy(vertices_list))
    communities = list()
    while remaining_vertices:
        node = remaining_vertices.pop()
        queue = [node]
        community = set()
        while queue:
            current_node = queue.pop(0)
            community.add(current_node)
            neighbours = vertex_dict[current_node]
            for neighbour in neighbours:
                if neighbour not in community:
                    queue.append(neighbour)
        remaining_vertices = remaining_vertices.difference(community)
        community = sorted(community)
        communities.append(community)
    return communities
        
#%%
def detect_communities(betweenness_list: list, vertex_dict: dict):
    edge_count = len(betweenness_list)
    betweenness_list_copy = deepcopy(betweenness_list)
    vertex_dict_copy = deepcopy(vertex_dict)
    configuration = dict()
    configuration["max_modularity"] = -1.001
    configuration["best_config"] = None
    while len(betweenness_list_copy):
        communities = generate_communities(list(vertex_dict_copy.keys()), vertex_dict_copy)
        modularity = get_modularity(vertex_dict_copy, communities, edge_count)
        if modularity > configuration["max_modularity"]:
            configuration["max_modularity"] = modularity
            configuration["best_config"] = communities
        vertex_dict_copy, betweenness_list_copy = remove_edges_update_graph(vertex_dict_copy, betweenness_list_copy)
    return configuration["best_config"]
            
#%%
def get_edge_value(tree_data: dict) -> dict:
    visited = tree_data["visited"]
    parent_dict = tree_data["parent_dict"]
    diverging_paths = tree_data["diverging_paths"]
    node_values = dict()
    edge_values = dict()
    
    for node in visited:
        node_values[node] = 1
    for node in list(reversed(visited))[:-1]:
        for parent in parent_dict[node]:
            edge = tuple(sorted([node, parent]))
            credit = (node_values[node] * diverging_paths[parent]) / diverging_paths[node]
            node_values[parent] += credit
            try:
                edge_values[edge] += credit
            except KeyError:
                edge_values[edge] = 0
                edge_values[edge] += credit
    return edge_values

#%%
def calculate_betweenness(master_dict: dict) -> dict:
    betweenness_dict = dict()
    for edge_dict in master_dict.values():
        for edge in edge_dict.keys():
            try:
                betweenness_dict[edge] += edge_dict[edge] / 2
            except KeyError:
                betweenness_dict[edge] = 0
                betweenness_dict[edge] += edge_dict[edge] / 2
    return betweenness_dict

#%%
def filter_dissimilar_users(user_1: str, user_2: str)-> bool:
    user_1_set = set(user_dict[user_1])
    user_2_set = set(user_dict[user_2])
    len_intersection = len(user_1_set.intersection(user_2_set))
    if len_intersection < filter_threshold:
        return False
    else:
        return True

#%%
filter_threshold = 4
input_file_path = "data/test_task2_user_business.csv"
betweenness_output_file_path = "task2_betweenness.output"
community_output_file_path = "task2_community_new.output"
'''
filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
betweenness_output_file_path = sys.argv[3]
community_output_file_path = sys.argv[4]
'''
#%%
start = time.time()
conf = SparkConf().setAppName("Task-2").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlcontext = SQLContext(sc)

#%%
input_rdd = sc.textFile(input_file_path)
input_rdd = input_rdd.mapPartitions(lambda x: csv.reader(x)) # 1st column - user id, 2nd column - business id
headers = input_rdd.first()
input_rdd = input_rdd.filter(lambda x: x != headers)

#%%
input_rdd = input_rdd.groupByKey().map(lambda x: (x[0], set(x[1])))
user_dict = input_rdd.collectAsMap()
user_initial_rdd = input_rdd.map(lambda x: x[0])
pair_rdd = user_initial_rdd.cartesian(user_initial_rdd).filter(lambda x: x[0] != x[1]).filter(lambda x: filter_dissimilar_users(x[0], x[1]))
user_rdd = pair_rdd.flatMap(lambda x: x).map(lambda x: tuple([x])).distinct()

#%%
vertex_dict = pair_rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collectAsMap()

vertex_tree_data = user_rdd.map(lambda x: (x[0], build_tree_bfs(x[0], vertex_dict)))
edge_values_distributed = vertex_tree_data.map(lambda x: (x[0], get_edge_value(x[1]))).collectAsMap()

betweenness = calculate_betweenness(edge_values_distributed)
betweenness_list = sorted(betweenness.items(), key = (lambda x: (-x[1], x[0])))
write_betweenness_output(betweenness_list)

communities_detected = detect_communities(betweenness_list, vertex_dict)
communities_detected = sorted(communities_detected, key = lambda x: (len(x), x))

write_community_output(communities_detected)

#%%
sc.stop()
print ("\nDuration:" + str(time.time() - start))
