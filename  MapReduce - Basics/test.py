# -*- coding: utf-8 -*-

import sys
import orjson as json

review_file = 'data/review.json'
business_file = 'data/business.json'
output_file = 'no_spark_op_task2.json'
if_spark = 'no_spark'
top_n_categories = 50

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

# review_data = []
with open(review_file) as f:
    data_dict = {}
    for line in f:
        review_dict = json.loads(line)
        if review_dict["business_id"] in data_dict:
            data_dict[review_dict["business_id"]].append(float(review_dict["stars"]))
        else:
            data_dict[review_dict["business_id"]] = [float(review_dict["stars"])]


# master_dict = {}
# with open(business_file) as f:
#     business_dict = {}
#     category_review_count = {}
#     for line in f:
#         business_dict = json.loads(line)
        
#         if business_dict["categories"] is not None:
#             for category in business_dict["categories"].split(','):
#                 category = category.strip()
#                 try:
#                     if category not in category_review_count:
#                         category_review_count[category] = data_dict[business_dict["business_id"]]
#                     else:
#                         category_review_count[category] += data_dict[business_dict["business_id"]]
#                 except KeyError:
#                     continue
#         else:
#             category = "N"
#             try:
#                 if category not in category_review_count:
#                         category_review_count[category] = data_dict[business_dict["business_id"]]
#                 else:
#                     category_review_count[category] += data_dict[business_dict["business_id"]]
#             except KeyError:
#                 continue



"""-------------------------------------------------"""
with open(business_file) as f:
    business_dict = {}
    category_review_count = {}
    for line in f:
        business_dict = json.loads(line)
        if business_dict["business_id"] in data_dict:
            if business_dict["categories"]:
                for category in business_dict["categories"].split(","):
                    category = category.strip()
                    if category not in category_review_count:
                        category_review_count[category] = data_dict[business_dict["business_id"]]
                    else:
                        category_review_count[category] += data_dict[business_dict["business_id"]]
            else:
                category = "N"
                if category not in category_review_count:
                    category_review_count[category] = data_dict[business_dict["business_id"]]
                else:
                    category_review_count[category] += data_dict[business_dict["business_id"]]

        

category_review_count1 = {}
for key in category_review_count:
    category_review_count1[key] = sum(category_review_count[key])
    
category_review_count1 = sorted(category_review_count1.items(), key=lambda x: (-x[1], x[0]), reverse=False)
 
import json
write_output(category_review_count1, top_n_categories)

###
with open(business_file) as f:
    business_list = []
    business_dict = {}
    business = set()
    for line in f:
        business_dict = json.loads(line)
        business.add(business_dict["business_id"])
        business_list.append(business_dict["business_id"])
        
# ###
# count = 0
# for key in data_dict:
#     count += len(data_dict[key])


####################################

business_category_dict = {}

with open(business_file) as f:
    business_dict = {}
    category_review_count = {}
    for line in f:
        business_dict = json.loads(line)
        category_list = []
        if business_dict["categories"]:
            for category in business_dict["categories"].split(","):
                category = category.strip()
                category_list.append(category)
        else:
            category_list.append('N')
        
        business_id = business_dict["business_id"]
        business_category_dict[business_id] = category_list
        


category_review_dict = {}
with open(review_file) as f:
    # data_dict = {}
    review_dict = {}
    for line in f:
        review_dict = json.loads(line)
        
        business_id = review_dict["business_id"]
        if business_id not in business_category_dict:
            continue
        
        category_list = business_category_dict[business_id]
        
        for category in category_list:
            if category in category_review_dict:
                category_review_tuple = category_review_dict[category]
                category_review_tuple[0] +=1
                category_review_tuple[1]+= float(review_dict["stars"])
                category_review_dict[category] = category_review_tuple
            else:
                category_review_tuple = [1, float(review_dict["stars"])]
                category_review_dict[category] = category_review_tuple
        

        
ans = {}
for category in category_review_dict:
    category_review_tuple = category_review_dict[category]
    average = category_review_tuple[1] / category_review_tuple[0]
    ans[category] = average


    
output = sorted(ans.items(), key=lambda x: (-x[1], x[0]), reverse=False)        
import json   
write_output(output, top_n_categories)
