#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 09:11:31 2020

@author: vanish
"""
import string

mystr = "Five of us gathered for a Friday night dinner at Glutton.  Chef Bradley Manchester cooked for the table and just sent out dishes from the fall menu he wanted us to try.  We got:\n\nCayenne and cheddar pork rinds\nEggplant caponata with goat cheese toast\nBrandade Rangoon with salt cod\nGuanciale flatbread\nShaved Brussels sprouts salad\nBeet salad\nJapanese street corn\nPrime flank steak\nGnocchi with pork cheek\nShort rib cavatelli\nButternut squash agnolotti\nWood roasted chicken\nTurkey and guanciale meatballs\nGrilled bread\nMaple and mascarpone cheesecake\nChurros and hot chocolate\nCaramel apple fritters\nCaramel corn profiteroles\n\nThis restaurant is well named for the food can make one become a glutton if the food here is not consumed sparingly instead of a large order like ours.  Execution of the dishes was excellent.  There was nothing from the list that I didn't think was great.\n\nStandouts--the Japanese street corn is a must get.  The shaved Brussels sprouts salad was superb and all four desserts were very good.  The gnocchi with pork cheek was sublime!\n\nService was excellent with fresh share plates and silverware being brought.  For a newer restaurant in DTLV, this is a standout among the ones I've been to.  One caveat--we had the gnocchi but that is not on the fall menu.  If it returns to the menu during another season, it's a must get!"

words = mystr.split()

text = "while everyone else is busy catching pokemon, i am busy finding the best place to get poke in town. poke burrito concoction is becoming a big thing with the recent opening of soho sushi bowlrrito, pokeman, to see roll and sweet poke. with so many sushi burrito places opening up, the biggest question is...what makes each shops different from each other? is it the quality? quantity? price? service? sauce? from the looks of it, seems that they all have --similar toppings with similar protein choices at similar price ranges. on my visit to sweet poke, i constantly found myself comparing this place to my favorite place, pokeman. price was $1-2 cheaper. same great quality fish but i actually got more. i ordered their regular burrito and they generously packed 4 scoops of salmon in my burrito. with other toppings included, they packed so much that they couldn't even roll my fat burrito. they did, but seaweed was split in half with rice and toppings spilling out from all over the place. this much food and i paid around $9 with tax. what a deal. my burrito turned out to be one ginormous but delicious burrito that i am drooling over as i write this review thinking about it. bowl seemed to be a bit disappointing. we got a large and regular bowl. difference was that large comes with 4 scoops of rice while regular comes with 3. it felt like they were about the same portions which was a bit odd. paid more, yet portion was exactly the same. i did like how it came in a microwave safe bowl that can be reused at home. great for packing your lunch to school or work. i will be back here. great quality, great price and friendly service. i'm a fan."

text = text.translate(str.maketrans('', '', string.punctuation))

text = "hey what'ca doing"

words = text.split()

ans = []
for word in words:
    word = word.split("'")
    w = word.translate(str.maketrans('', '', string.punctuation))
    ans.append(w)
   
   
   
   