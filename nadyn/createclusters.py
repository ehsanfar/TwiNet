from .clusters import Cluster
from os.path import dirname, abspath, isfile
import os
import json
import csv
import re
from collections import defaultdict
import sys
import pickle
from itertools import chain
maxInt = sys.maxsize
csv.field_size_limit(maxInt)

def createUseridtypefiles(): 
    d = dirname(dirname(abspath(__file__))) + '/dataset/'
    # if isfile(d + 'user_type_dict.json'): 
    #     return  
    # user_id_dict = defaultdict(int)
    with open(d + 'user_id_dict.json', 'rt', encoding = 'latin1') as inoutfile:
        user_id_dict = json.load(inoutfile) 

    user_id_dict['BrianMBendis'] = 16395449
    user_id_dict['robdelaney'] = 22084427
    user_id_dict['stephenfry'] = 15439395
    user_id_dict['CNN'] = 759251
    user_id_dict['kfile'] = 326255267
    user_id_dict['dominiccampbell'] = 7526892 
    
    for user, v in list(user_id_dict.items()): 
        user_id_dict[user.lower()] = v
    
    with open(d + 'user_id_dict.json', 'w') as inoutfile:
        json.dump(user_id_dict, inoutfile)

        
    type1_namelist = []
    with open(d + 'twitter_accounts_journals.txt', 'rt', encoding = 'utf-16-le') as infile: 
        for row in infile:
            row = row.strip()
            tempre = re.search(r'.*@(.+)$', row)
            if tempre: 
                # print(row,tempre)
                name = tempre.group(1).rstrip().lower()
                print("row, name:", row, name)
                # name = tempre.group(1).lower()
                type1_namelist.append(user_id_dict[name])
    type2_namelist = []
    with open(d + 'twitter_accounts_individuals.txt', 'rt', encoding = 'utf-16-le') as infile:
        for row in infile: 
            row = row.strip()
            tempre = re.search(r'.*@(.+)$', row)
            if tempre: 
                name = tempre.group(1).rstrip().lower()
                print("row, name:", row, name)      
                type2_namelist.append(user_id_dict[name])
    
    user_type_dict = {1: [int(e) for e in type1_namelist], 2:[int(e) for e in type2_namelist]}
    with open(d + 'user_type_dict.json', 'w') as outfile: 
        json.dump(user_type_dict, outfile)

                    

def createClusters():
    createUseridtypefiles()
    print('something')
    clusterDict = {}
    d = dirname(dirname(abspath(__file__))) + '/dataset/'
    # filepath = d + "/user_doctorow_retweeters.dat"
    # print(filepath)
    # print(os.path.getsize(filepath))
    with open(d + 'user_type_revised.json', 'r') as infile:
        user_type_revised = json.load(infile)

    user_type_revised = {i: [int(e) for e in l] for i,l in user_type_revised.items()}
    
    with open(d + 'status_retweeter_dict.json', 'r') as infile: 
        status_retweeter_dict = json.load(infile)
    
    status_retweeter_dict = {int(j): [int(e) for e in l] for j,l in status_retweeter_dict.items()}
    status_retweeter_dict = defaultdict(list, status_retweeter_dict)
    print("status retwitter dictionary size:", len(status_retweeter_dict))
    for file in os.listdir(d):
        # print(file)
        if file.endswith(".csv") and file.startswith('general_clusters'):
            tempsearch = re.search(r'.+_2017-(\d+)-(\d+)_end_2017-(\d+)-(\d+)_.+', file)
            m1, d1, m2, d2 = tempsearch.groups()
            # print(m1,d1,m2,d2)
            filepath = d + file
            cluster_statuses_dict = defaultdict(list)
            # try:
            # print(filepath)
            with open(filepath, "r", encoding = 'utf-8') as f:
                csvfile = list(csv.DictReader(f))
                # print(len(list(csvfile)))
                clusterset = set([int(row['cluster']) for row in csvfile if row['cluster']])
                cluster_statusset = set([int(row['id']) for row in csvfile if row['id']])
                # print("cluster set:", clusterset)
                for cl in clusterset:
                    clusterid =  '%s%s%s%s_%s'%(str(m1),str(d1).zfill(2),str(m2).zfill(2),str(d2).zfill(2), str(cl).zfill(2))
                    clusterDict[clusterid] = Cluster(clusterid, user_type_revised['1'], user_type_revised['2'])
                
                
                for row in csvfile:
                    if not row['cluster']: 
                        continue
                    clusternum = int(row['cluster'])
                    clusterid =  '%s%s%s%s_%s'%(str(m1),str(d1).zfill(2),str(m2).zfill(2),str(d2).zfill(2), str(clusternum).zfill(2))
                    status = int(row['id'])
                    statustext = row['text']
                    
                    if not statustext.startswith('RT @'):
                        clusterDict[clusterid].addStatus(status, int(row['user']), row['datetime'])  
                        clusterDict[clusterid].addRetweeters(status, status_retweeter_dict[status])
                            
    print("length of clusters:", len(clusterDict))
    # print(clusterDict)
    retweeters = []
    statuses = []
    for id, cl in clusterDict.items(): 
        # if len(cl.statususer_one) + len(cl.statususer_two)> 10: 
        #     print(id, len(cl.statususer_one), len(cl.statususer_two))
        #     print("size of users:", len(cl.userretweeters_one), len(cl.userretweeters_two))
        for u, r in chain(cl.userretweeters_one.items(), cl.userretweeters_two.items()):
            retweeters.extend(r)
    
        statuses.extend(chain(cl.statususer_one.keys(), cl.statususer_two.keys()))        
        # print("size of retweeters:")
        # print("user set one:", [(u, len(v)) for u, v in cl.userretweeters_one.items()])
        # print("user set two:", [(u, len(v)) for u,v in cl.userretweeters_two.items()])    
    print('length of retweeters in clusters:', len(retweeters))
    print('length of statuses in clusters:', len(statuses))
    
    with open('clusternameClusterDict.pickle', 'wb') as outfile:
        pickle.dump(clusterDict, outfile)
    
                     
            

                    
            
            # except EOFError:
            #     pass


