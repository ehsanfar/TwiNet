import os
from os.path import dirname, abspath
import json
import networkx as nx

from collections import defaultdict, Counter
import matplotlib.pyplot as plt



def updateGraph(G, source_id, retweeterlist):
    G.add_node(source_id)
    # print(retweeterlist)
    retweeterdict = Counter(retweeterlist)
    # print(retweeterdict)
    edges_weight = []
    for r, w in retweeterdict.items():
        # if w < 2:
        #     continue
        retid = int(r)
        edges_weight.append(tuple([retid, source_id, w]))

    G.add_weighted_edges_from(edges_weight)



def createNetwork():
    d = dirname(dirname(abspath(__file__))) + '/dataset/'

    with open(d + 'status_user_dict.json', 'r') as infile:
        status_user_dict = json.load(infile)

    with open(d + 'user_retweeter_dict.json', 'r') as infile:
        user_retweeter_dict = json.load(infile)

    # with open(d + 'useridtype_dict_revised.json', 'r') as infile:
    #     useridtype_dict = json.load(infile)

    with open(d + 'user_id_dict.json', 'r') as jsonfile:
        user_id_dict = json.load(jsonfile)


    with open(d + 'user_type_dict.json', 'r') as jsonfile:
        user_type_dict = json.load(jsonfile)






    user_id_dict = {k: int(v) for k ,v in user_id_dict.items()}
    # user_id_dict["michaelianblack"] = 21035409
    # user_id_dict["TheEllenShow"] =  15846407
    # user_id_dict["CraigyFerg"] = 112508240
    # user_id_dict["bheater"] = 15741636
    # user_id_dict["hodgman"] = 14348594


    for name, id in list(user_id_dict.items()):
        user_id_dict[id] = name


    # print("Number of users with status:", len(set(status_user_dict.values())))
    userset = set(status_user_dict.values())
    interuser_retweeterdict = defaultdict(list, {int(u): set([e for e in li if e in userset]) for u, li in user_retweeter_dict.items()})
    # print([len(v) for v in interuser_retweeterdict.values()])

    G = nx.DiGraph()
    for sourceuser, retuserlist in interuser_retweeterdict.items():
        updateGraph(G, sourceuser, retuserlist)

    nodelist1 = user_type_dict["1"]
    nodelist2= user_type_dict["2"]


    plt.figure()
    pos = nx.spring_layout(G)


    nx.draw_networkx_nodes(G, pos, nodelist= [e for e in G.nodes() if e in nodelist1],node_size= 100, cmap=plt.get_cmap('jet'), node_color= 'red')
    nx.draw_networkx_nodes(G, pos, nodelist= [e for e in G.nodes() if e not in nodelist1],node_size= 100, cmap=plt.get_cmap('jet'), node_color= 'blue')

    # nx.draw_networkx_labels(G, pos, labels = user_id_dict)
    # nx.draw(G)
    nx.draw_networkx_edges(G, pos, edge_color='k', arrows=False)
    plt.show()

    # calcualte centrality
    centrality_eigen = nx.eigenvector_centrality(G)
    print(sorted([(user_id_dict[node], centrality_eigen[node]) for node in centrality_eigen], key = lambda x: x[1], reverse= True))
    centrality_degree = nx.degree_centrality(G)
    print(sorted([(user_id_dict[node], centrality_degree[node]) for node in centrality_degree], key = lambda x: x[1], reverse= True))
    centrality_indegree = nx.in_degree_centrality(G)
    print(sorted([(user_id_dict[node], centrality_indegree[node]) for node in centrality_indegree], key = lambda x: x[1], reverse= True))
    centrality_outdegree = nx.out_degree_centrality(G)
    print(sorted([(user_id_dict[node], centrality_outdegree[node]) for node in centrality_outdegree], key = lambda x: x[1], reverse= True))

    indegree_outdegree_node_tuple = [(centrality_indegree[k], centrality_outdegree[k], user_id_dict[k]) for k in G.nodes()]
    print(sorted(indegree_outdegree_node_tuple, reverse= True))
    plt.figure(2)
    plt.scatter([e[0] for e in indegree_outdegree_node_tuple], [e[1] for e in indegree_outdegree_node_tuple])
    # plt.
    plt.show()


    #
    # plt.figure(3)
    #
    # plt.figure(4)












