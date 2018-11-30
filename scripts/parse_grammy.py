import os, codecs, json
import pandas as pd

datapath = '..\\data\\raw\\grammyData.json'
grammy = json.load(codecs.open(datapath, 'r'))
print (len(grammy))


award_types = {}
for i in grammy:
	if i['category'] not in award_types:
		award_types.setdefault(i['category'], set())
	award_types[i['category']].add(i['annualGrammy'])

print (award_types)
print (len(award_types))

d = {}
d['year'] = [] # 60th Grammy is awarded in Jan 2018, so actually it summarizes the performance of the song in 2017
d['name'] = [] #the person who claim the award
d['award_for'] = [] #the song, album or the artist
d['category'] = []
d['award_type'] = []
for i in grammy:
	d['year'].append(i['annualGrammy']+2017-60)
	d['name'].append(i['name'])
	d['award_for'].append(i['awardFor'])
	d['category'].append(i['category'])
	d['award_type'].append(i['awardType'])

df = pd.DataFrame.from_dict(d)
df.to_csv('..\\data\\extracted\\grammy.csv')