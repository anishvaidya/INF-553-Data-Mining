# -*- coding: utf-8 -*-
import orjson as json

with open("task3.json") as f:
    for line in f:
        default = json.loads(line)


with open("task3_custom.json") as f:
    for line in f:
        custom = json.loads(line)
        
custom_dict = {}
for item in custom["result"]:
    custom_dict[item[0]] = item[1]
    
default_dict = {}
for item in default["result"]:
    default_dict[item[0]] = item[1]
    
count = 0
for key in custom_dict:
    if custom_dict[key] == default_dict[key]:
        count += 1