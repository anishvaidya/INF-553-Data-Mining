# -*- coding: utf-8 -*-
from itertools import combinations, chain

mylist = [('100', '101'),('100', '102'),('100', '98'),('100', '99'),('101', '102'),('101', '97'),('101', '98'),('101', '99'),('102', '103'),('102', '105'),('102', '97'),('102', '98'),('102', '99'),('103', '105'),('103', '97'),('103', '98'),('103', '99'),('105', '98'),('105', '99'),('97', '98'),('97', '99'),('98', '99')]

# uni = set()
# for pair in mylist:
#     uni = uni.union(set(pair))

# triplets = []
# for pair in mylist:
#     for u in uni:
#         t = set(pair).add(u)
#         if len(t) == 3:
#             triplets.append(t)

n_items = 3
def get_combinations(itemset_list, n_items):
    combinations_list = set()

    for item1 in itemset_list:
        for item2 in itemset_list:
            item_union = set(item1).union(set(item2))
            if len(item_union) == n_items:
                combinations_list.add(tuple(sorted(item_union)))
    return sorted(combinations_list)

res = get_combinations(mylist, n_items)

# frequent_singles = sorted(set(chain(*mylist)))
# res2 = sorted(list(combinations(frequent_singles, l+1)))

# res3 = set(res2)

new_candidates = {tuple(sorted(set(i) | set(j))): 0
                          for i in mylist for j in mylist if len(set(i) | set(j)) == n_items}