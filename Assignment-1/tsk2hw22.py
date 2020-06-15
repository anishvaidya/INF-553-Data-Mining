# -*- coding: utf-8 -*-
"""
Created on Thu May 28 06:21:10 2020

@author: pratu
"""

import json
import operator
#f = open('review.json','r') 
#f1=open('business.json','r')
#data=json.loads(f)

#with open('review.json') as f:
#    data=json.loads("["+f.read().replace("}\n)])
#data1=json.loads(f['business_id'],f['categories'])

#for i in data:
#    print(data[i])
my_dict={}
list1=[]
with open('data/business.json','r',encoding='utf8') as f:
    for line in f:
        v=json.loads(line)
        if v['business_id'] is not None:
            k=v['business_id']
            if v['categories'] is not None:
                  val=v['categories']
            else:
                val='p'
            my_dict[k]=val
        
        #break
categorydict={}        
#print(my_dict)
with open('data/review.json','r',encoding='utf8') as f1:
    for line in f1:
        #print(line)
        v1=json.loads(line)
        #print('b')
        if v1['business_id'] is not None:
            k1=v1['business_id']
            #print(k1,'a')
            if my_dict.get(k1) is not None:
            
             c=my_dict[k1]
             c=c.split(',')
            #print(c)
             for i in list(c):
                #print(i)
                i = i.strip()
                if i not in categorydict:
                    #i=i.strip()
                    count=0  
                    list1=[]
                    list1.append(1)
                    if v1['stars'] is not None:
                       list1.append(v1['stars'])
                    else:
                      list1.append(0)
                 #print(v1['stars'])
                    categorydict[i]= list1 
                else:
                    categorydict[i][0]+=1
                    if v1['stars'] is not None:
                      categorydict[i][1]+=v1['stars']
                    else:
                        categorydict[i][1]+=0
                #print(categorydict[i]) 
                #print(k1)
                #print(c)
        
cat={} 
print(categorydict['Nudist']) 
for k,v  in categorydict.items(): 
   
   i=v[1]/v[0] 
   cat[k]=i
cat1=sorted(cat.items(), key=lambda x: [-x[1], x[0]])
#fruit = sorted(cat.items(), key=operator.itemgetter(0))
#sorted(fruit, key=operator.itemgetter(1), reverse=True)
#print(dict(list(cat.items())[0:2]))
#print(cat['Astrologers'])
#print(cat1[0:5])
#print(cat[0:5])
cat3=cat1[0:50]
print(cat3)
cat2=[]
for i in cat1:
    j=i[0].strip()
    cat2.append((j,i[1]))
    
cat2=cat2[0:50]    
#print(cat2[0:5])
cat2=str(cat2) 
cat2=cat2.replace('(','[').replace(')',']')
cat2=cat2.replace("'", '"')
#print(cat2)
 
#result={}
#result["result"]=cat2 
#with open('task2_no_spark_ans', "w") as f:
#        json.dump(result, f) 
##   
f = open('task2_no_spark_ans', 'w')
f.write("{\"result\": "+cat2+"}")
f.close()
#cat2= sorted(cat1.items(),key=operator.itemgetter(1),reverse=True)           
#print(cat)            