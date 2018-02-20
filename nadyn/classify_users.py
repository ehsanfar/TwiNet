import os
from os.path import dirname, abspath
import json
import networkx as nx
import re
from .classified import Classified
import pickle
import random
from collections import defaultdict

def swatUsers(userset_one, userset_two, user_retweeterset_dict): 
    users = list(userset_one.union(userset_two))
    retw_count_one = defaultdict(int)
    retw_count_two = defaultdict(int)
    
    for u, retset in user_retweeterset_dict.items(): 
        if u in userset_one: 
            for s in retset: 
                retw_count_one[s] += 1
        elif u in userset_two: 
            for s in retset: 
                retw_count_two[s] += 1
    
    print("count done")
    user_sharedrets_dict = {}
    for user in users: 
        retweeterset = user_retweeterset_dict[user]
        shared1 = [retw_count_one[r] for r in retweeterset if retw_count_one[r]>1]
        shared2 = [retw_count_two[r] for r in retweeterset if retw_count_two[r]>=1]
        user_sharedrets_dict[user] = (sum(shared1)+len(shared1), sum(shared2)+len(shared2))
    
    print("user shared retweets done")
    revised_one = set([])
    revised_two = set([])
    for u in users: 
        if user_sharedrets_dict[u][0]>user_sharedrets_dict[u][1]: 
            revised_one.add(u)
        elif user_sharedrets_dict[u][1]>user_sharedrets_dict[u][0]:
            revised_two.add(u)
    print('user revised done part I')
    for u in users: 
        if u not in revised_one and u not in revised_two: 
            print("None assigned user")
            if len(revised_one)>=len(revised_two): 
                revised_two.add(u)
            else: 
                revised_one.add(u)
    
    print('user revised doen part 2')
    return (revised_one, revised_two)
    
    
def classifyUserRetweeterMethod(): 
    d = dirname(dirname(abspath(__file__))) + '/dataset/'
    
    # with open(d + 'status_user_dict.json', 'r') as infile:
    #     status_user_dict = json.load(infile)
    
    # with open(d + 'status_retweeter_dict.json', 'r') as infile: 
    #     status_retweeter_dict = json.load(infile)
    with open(d + 'user_retweeter_dict.json', 'r') as infile:
        user_retweeter_dict = json.load(infile)

    with open(d + 'useridtype_dict.json', 'r') as infile:
        initialtype_dict = json.load(infile)
    
    # print("number of users:", len(user_retweeter_dict))
    # print("number of retweeters:", sum([len(set(v)) for v in user_retweeter_dict.values()]))
    users = [int(e) for e in initialtype_dict['1'] + initialtype_dict['2']]
    initialtype_dict = {u: [int(e) for e in v] for u, v in initialtype_dict.items()}
    user_retweeterset_dict = defaultdict(set, {int(u): set(v) for u, v in user_retweeter_dict.items()})
    user_retweeterset_list = sorted(user_retweeterset_dict.items())
    
    set1 = set([])
    set2 = set([])
    
    for u, s in user_retweeterset_dict.items(): 
        if u in initialtype_dict['1']: 
            set1 = set1.union(s)
        elif u in initialtype_dict['2']: 
            set2 = set2.union(s)
    
    print("length of all sets :", sum([len(s) for s in user_retweeterset_dict.values()]))
    print("length set 1 and set 2 and intersection:", len(set1), len(set2), len(set1.intersection(set2)))
    
    uset1 = set([int(e) for e in initialtype_dict['1']])
    uset2 = set([int(e) for e in initialtype_dict['2']])
    newuset1 = set([])
    newuset2 = set([])
    print("length of first sets:", len(uset1), len(uset2))
    # for u in users: 
    #     print(len(user_retweeterset_dict[u]))
    i = 0 
    while i<=20:
        i += 1 
        newuset1, newuset2 =  swatUsers(uset1, uset2, user_retweeterset_dict)
        if newuset1 == uset1 and newuset2 == uset2: 
            break 
        print("length of new sets:", len(newuset1), len(newuset2))
        set1 = set([])
        set2 = set([])
        
        for u, s in user_retweeterset_dict.items(): 
            if u in newuset1: 
                set1 = set1.union(s)
            elif u in newuset2: 
                set2 = set2.union(s)
        
        print("length of all sets :", sum([len(s) for s in user_retweeterset_dict.values()]))
        print("length set 1 and set 2 and intersection:", len(set1), len(set2), len(set1.intersection(set2)))
        # input("press any key ...")
        uset1 = newuset1
        uset2 = newuset2
    
    user_type_revised = {'1': list(newuset1), '2': list(newuset2)}
    with open(d + 'user_type_revised.json', 'w') as outfile:
        json.dump(user_type_revised, outfile)
    # sample_class_dictionary = {'user1':, 'user2':, 'retsize1':, 'retsize2':, 'retsharedsize': }
    # ratio = 0.5
    # classified_list = []
    # for i in range(10000): 
    #     retset1 = set([])
    #     retset2 = set([])
    #     tempuser1 = random.sample(users, int(ratio*len(users)))
    #     tempuser2 = [u for u in users if u not in tempuser1]
        
    #     for u, s in user_retweeterset_dict.items(): 
    #         if u in tempuser1: 
    #             retset1 = retset1.union(s)
    #         elif u in tempuser2: 
    #             retset2 = retset2.union(s)
        
    #     sharedset = retset1.intersection(retset2)
    #     print(i, len(retset1), len(retset2), len(sharedset))
    #     classified_list.append(Classified(tempuser1, tempuser2, len(retset1), len(retset2), len(sharedset)))
    #     if (i+1)%1000 == 0:
    #         with open(d + 'classifieds_rawsamples_%1.1f.pickle'%ratio, 'wb') as outfile:
    #             pickle.dump(classified_list, outfile)
                
    # with open(d + 'classifieds_rawsamples_%1.1f.pickle'%ratio, 'wb') as outfile:
    #     pickle.dump(classified_list, outfile)

def userClusterMethod():
    user_id_dict = {}
    with open('user_id_dict.json', 'r') as jsonfile:
        user_id_dict = json.load(jsonfile)

    user_id_dict['BrianMBendis'] = 16395449
    user_id_dict['robdelaney'] = 22084427
    user_id_dict['stephenfry'] = 15439395

    for name, id in user_id_dict.items():
        user_id_dict[id] = name

    all_ids = user_id_dict.keys()
    print(all_ids)

    def create_graph():
        global user_id_dict
        dir = dirname(abspath(__file__))
        G = nx.Graph()
        def updateGraph(source_id, retweeters_dict):
            G.add_node(source_id)
            edges_weight = []
            for r, w in retweeters_dict:
                if w<2:
                    continue
                retid = int(r)
                edges_weight.append(tuple([retid, source_id, 1]))

            G.add_weighted_edges_from(edges_weight)

        for file in os.listdir(dir):
            reusername = re.search(r'user_(.+)_retweeters.json', file)
            if file.endswith(".json") and reusername:
                print(file)
                screen_name = reusername.group(1)
                source_id = user_id_dict[screen_name]
                print(screen_name, source_id)
                with open(file, 'r') as infile:
                    retweeters = json.load(infile)



                updateGraph(source_id, retweeters)
                print(file[5:-16], retweeters)

        nx.write_gpickle(G, 'retweeter_graph')

    create_graph()
    G = nx.read_gpickle('retweeter_graph')

    print(len(G.nodes()))
    print(len(G.edges()))


    from hac import GreedyAgglomerativeClusterer
    clusterer = GreedyAgglomerativeClusterer()

    karate_dendrogram = clusterer.cluster(G)
    karate_dendrogram.clusters(1)
    cluster2 = karate_dendrogram.clusters(2)
    # print [len(a) for a in cluster2]

    user_set = set(list(user_id_dict.values()))

    u1 = cluster2[0]
    u2 = cluster2[1]

    print([user_id_dict[e] for e in list(user_set.intersection(u1))])
    print([user_id_dict[e] for e in list(user_set.intersection(u2))])



