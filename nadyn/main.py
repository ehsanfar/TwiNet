import os
import cPickle
from os.path import dirname, abspath
import json
import networkx as nx
import re


clusterfilenames = (
    "general_clusters_date_begin_2017-06-01_end_2017-06-04_tweets.csv"
    
)
user_id_dict = {}
with open('user_id_dict.json', 'r') as jsonfile:
    user_id_dict = json.load(jsonfile)

user_id_dict['BrianMBendis'] = 16395449
user_id_dict['robdelaney'] = 22084427
user_id_dict['stephenfry'] = 15439395

for name, id in user_id_dict.items():
    user_id_dict[id] = name


def readalltweets():
    d = dirname(dirname(abspath(__file__)))
    with open(d + '/all_tweets_retweeters_collected.dat', 'r') as infile:
        sample_data = cPickle.load(infile)

    print sample_data[:10]
    print type(sample_data)
    print len(sample_data)

# def readclusters():



# readalltweets()
# for file in os.listdir(dir):
#     # reusername = re.search(r'user_(.+)_retweeters.json', file)
#     if file.endswith(".dat") and file.startswith("general_clusters"):
#         print(file)
#         screen_name = reusername.group(1)
#         source_id = user_id_dict[screen_name]
#         print(screen_name, source_id)
#         with open(file, 'r') as infile:
#             retweeters = json.load(infile)
#
#         updateGraph(source_id, retweeters)
#         print(file[5:-16], retweeters)
#
#     nx.write_gpickle(G, 'retweeter_graph')
