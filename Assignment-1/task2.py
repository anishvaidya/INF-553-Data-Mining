# -*- coding: utf-8 -*-
import sys
import pyspark
from pyspark import SparkConf, SparkContext
import json

review_file = 'data/review.json'
business_file = 'data/business.json'
output_file = 'op_task2.json'
if_spark = 'spark'
top_n_categories = 50

'''
review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
top_n_categories = int(sys.argv[5])
'''

def create_tuples(x):
    list_of_tuples = []
    for xtype in x[0]:
        list_of_tuples.append( (xtype.strip(), x[1]) )
    return list_of_tuples


# def write_output(top_rated_categories):
#     outerlist = []
#     answer = {}
#     for row in top_rated_categories:
#         innerlist = []
#         innerlist.append(row[0])
#         innerlist.append(row[1])
#         outerlist.append(innerlist)
#     answer["result"] = outerlist
#     with open(output_file, 'w+') as fp:
#         json.dump(answer, fp)

def write_output(top_rated_categories, top_n_categories):
    outerlist = []
    answer = {}
    count = 0
    for row in top_rated_categories:
        if count < top_n_categories:
            innerlist = []
            innerlist.append(row[0])
            innerlist.append(row[1])
            outerlist.append(innerlist)
            count += 1
        else:
            break
    answer["result"] = outerlist
    with open(output_file, 'w+') as fp:
        json.dump(answer, fp)

def vanilla_runtime():
    with open(review_file) as f:
        data_dict = {}
        for line in f:
            review_dict = json.loads(line)
            if review_dict["business_id"] in data_dict:
                data_dict[review_dict["business_id"]].append(review_dict["stars"])
            else:
                data_dict[review_dict["business_id"]] = [review_dict["stars"]]

    with open(business_file) as f:
        business_dict = {}
        category_review_count = {}
        for line in f:
            business_dict = json.loads(line)
            
            if business_dict["categories"]:
                for category in business_dict["categories"].split(','):
                    category = category.strip()
                    try:
                        if category not in category_review_count:
                            category_review_count[category] = data_dict[business_dict["business_id"]]
                        else:
                            category_review_count[category] += data_dict[business_dict["business_id"]]
                    except KeyError:
                        continue
            else:
                category = "N"
                try:
                    if category not in category_review_count:
                            category_review_count[category] = data_dict[business_dict["business_id"]]
                    else:
                        category_review_count[category] += data_dict[business_dict["business_id"]]
                except KeyError:
                    continue


    for key in category_review_count:
        category_review_count[key] = sum(category_review_count[key]) / len(category_review_count[key])
        
    category_review_count = sorted(category_review_count.items(), key=lambda x: (-x[1], x[0]), reverse=False)
    write_output(category_review_count, top_n_categories)


def spark_runtime():
    conf = SparkConf().setAppName("app")
    sc = SparkContext(conf=conf)
    
    
    
    review_data = sc.textFile(review_file)
    business_data = sc.textFile(business_file)
    
    review_rdd = review_data.map(json.loads).map(lambda row: (row["business_id"], row["stars"])).cache()
    business_rdd = business_data.map(json.loads).map(lambda row: (row["business_id"], row["categories"])).cache()
    
    joined_rdd = business_rdd.join(review_rdd).map(lambda x: (x[1][0], x[1][1])).cache()
    
    joined_rdd = joined_rdd.map(lambda x: (x[0].split(',') if x[0] is not None else "N", x[1]))
    
    
    # joined_rdd1 = joined_rdd.map(lambda x: create_tuples(x)).collect() this works
    
    joined_rdd_tuples = joined_rdd.map(lambda x: create_tuples(x))
    
    joined_rdd_tuples1 = joined_rdd_tuples.flatMap(lambda x: x)
    
    # ourlist = joined_rdd_tuples1.collect()
    
    aggregated_tuples = joined_rdd_tuples1.groupByKey().mapValues(lambda x: sum(x)/len(x)).cache()
    
    top_rated_categories = aggregated_tuples.sortBy(lambda x: (-x[1], x[0])).take(top_n_categories)
    # top_rated_categories_sum = aggregated_tuples_sum.sortBy(lambda x: (-x[1], x[0])).take(top_n_categories)
    
    
    write_output(top_rated_categories, top_n_categories)
    # write_output(top_rated_categories_sum, top_n_categories)

if __name__ == "__main__":
    if if_spark == "spark":
        spark_runtime()
    elif if_spark == "no_spark":
        vanilla_runtime()