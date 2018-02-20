import sys
import numpy as np
import pymysql.cursors
from collections import defaultdict, Counter
from os.path import dirname, abspath, isfile, isdir, join
from os import listdir
import json
from datetime import datetime, timedelta
import pickle
import random
import time, datetime
import re
import csv
import statistics
from scipy.sparse import bsr_matrix

import sklearn.cluster as cluster

d = dirname(dirname(abspath(__file__))) + '/dataset/'
if isdir("/Users/abbasehsanfar/"):
    dir_Download = "/Users/abbasehsanfar/Downloads/windows/"
elif isdir("/Users/ccclab/"):
    dir_Download = "/Users/ccclab/Downloads/windows/"
else:
    dir_Download = d



conn2 = pymysql.connect(host='localhost',
                           user='root',
                           password='1574',
                           db='twitternetwork',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


class Window():
    def __init__(self, window, starttime, keystr):
        global max_index
        self.firsttime = starttime
        self.keystr = keystr
        self.days = window
        self.max_index = max_index
        self.wordcounter = Counter()
        self.connectioncounter = Counter()
        self.tweetcount = 0

    def addTweet(self, text):
        global word_count_dict, word_index_dict
        self.tweetcount += 1
        word_count_dict = Counter()
        wordlist = [word_index_dict[e] for e in text.split() if len(e) > 1 if word_index_dict[e]<self.max_index]
        # print(wordlist)
        for i, w1 in enumerate(wordlist):
            word_count_dict[w1] += 1
            for j, w2 in enumerate(wordlist[i:]):
                if w1 != w2:
                    tup = (min(w1, w2), max(w1, w2))
                    self.connectioncounter[tup] += 1

        self.wordcounter += word_count_dict


# class WindowState():
#     def __init__(self, firsttime, window):
#         global max_index
#         self.firsttime = firsttime
#         self.windows = window
#         self.max_index = max_index
#         self.wordcounterdict = defaultdict(Counter)
#         self.wordconnections = defaultdict(Counter)
#         self.tweetcountdict = Counter()
#
#     def addTweet(self, keystr, text):
#         global word_count_dict
#         self.tweetcountdict[keystr] += 1
#         word_count_dict = Counter()
#         wordlist = [word_index_dict[e] for e in text.split() if len(e) > 1 if word_index_dict[e]<self.max_index]
#         # print(wordlist)
#         for i, w1 in enumerate(wordlist):
#             word_count_dict[w1] += 1
#             for j, w2 in enumerate(wordlist[i:]):
#                 if w1 != w2:
#                     tup = (min(w1, w2), max(w1, w2))
#                     self.wordconnections[keystr][tup] += 1
#
#         self.wordcounterdict[keystr] += word_count_dict


def wordFreq():
    global conn2, d, word_count_dict, word_index_dict
    sql_read = "Select * from statustokens"
    word_count_dict = defaultdict(int)
    word_index_dict = defaultdict(int)
    with conn2.cursor() as cursor2:
        # cursor2.execute(sql_drop)
        # cursor2.execute(sql_cr)
        # conn2.commit()
        cursor2.execute(sql_read)
        result = cursor2.fetchall()

        for r in result:
            wordlist = [e.lower() for e in r['tokens'].split() if '@' not in e and len(e)>1]
            print(wordlist)
            for w in wordlist:
                word_count_dict[w] += 1

    wordssorted = [tup[0] for tup in sorted(word_count_dict.items(), key=lambda x: x[1], reverse=True)]
    word_index_dict = {w: i for i, w in enumerate(wordssorted)}


    with open(d + 'word_count_dict.json', 'w') as outfile:
        json.dump(word_count_dict, outfile)

    with open(d + 'word_index_dict.json', 'w') as outfile:
        json.dump(word_index_dict, outfile)

    with open(d + 'word_count_dict.json', 'r') as infile:
        word_count_dict = json.load(infile)


    for threshold in [5, 10, 25, 50]:
        temp_dict = {w:c for w,c in word_count_dict.items() if c>threshold}
        print("threshold & num_words:", threshold, len(temp_dict))


def timeWindow(window=1):
    global conn2, d
    sql_read = "Select created_at, tokens from statustokens where created_at> now()- interval %d day order by created_at"%interval
    # time = defaultdict(int)
    window_word_count_dict = defaultdict(Counter)
    bin = 0
    first = True
    firsttime = None
    winDict = {}

    with conn2.cursor() as cursor2:
        cursor2.execute(sql_read)
        result = cursor2.fetchall()
        # for i, r in list(enumerate(result))[:1000]:
        for i, r in list(enumerate(result)):
            if first:
                firsttime = r['created_at'].replace(hour = 0, minute = 0, second=0, microsecond=0)
                # winstateObj = WindowState(firsttime, window)
                first = False

            t = r['created_at']
            dt = t - firsttime
            keytime = firsttime + timedelta(days = window*(dt.days//window) + window//2)
            keystr = keytime.strftime("%Y-%m-%d")
            if keystr in winDict:
                winObj = winDict[keystr]
            else:
                starttime = firsttime + timedelta(days = window*(dt.days//window))
                winObj = Window(window, starttime = starttime, keystr = keystr)
                winDict[keystr] = winObj
            # print("time and keytime and window:", t, dt.days, window, window*(dt.days//window) + window//2, keystr)
            # if keytime in window_word_count_dict:

            winObj.addTweet(text = r['tokens'])
            if i %1000 == 0:
                print(i, r['tokens'])
            # else:
            #     window_word_count_dict[keytime] = word_count_dict
            # print(type(t), t, t + timedelta(days = 3))

            # time_str = datetime.datetime.strftime("%Y-%m-%d")
    # print(winstateObj.wordcounterdict)
    # print(winstateObj.wordconnections)
    print(sorted([(k, w.tweetcount) for k, w in winDict.items()]))
    print([(k, sum(w.wordcounter.values())) for k, w in sorted(winDict.items())])
    # print(window_word_count_dict)
    # with open(d + 'window_word_count_dict_w=%d.json'%window, 'w') as outfile:
    #     json.dump(window_word_count_dict, outfile)
    print("download directory name:", dir_Download)
    for keystr, winObj in winDict.items():
        pickle.dump(winObj, open(dir_Download + "window%s_%s.p"%(str(window).zfill(2), str(keystr)), "wb"))
    # pickle.dump(winstateObj, open(dir_Download + "windowStateObj_win%s.p"%str(window), "wb"))


def printClusters(labels):
    global index_word_dict
    counter = Counter(labels)
    cluster_word_dict = defaultdict(list)
    for i, l in enumerate(labels):
        if counter[l]< 10:
            cluster_word_dict[l].append(index_word_dict[i])

    for cluster, words in cluster_word_dict.items():
        print(cluster, words)


def clusterMatrix():
    global window, threshold, max_dim
    pattern = re.compile(r"window%s.+\.p"%str(window).zfill(2))
    filenames = [f for f in listdir(dir_Download) if isfile(join(dir_Download, f)) and pattern.match(f)]
    for f in sorted(filenames):
        tempwindow = pickle.load(open(dir_Download + f, "rb"))
        links = tempwindow.connectioncounter
        threshold_tuples = [(link, 1) for link,c in links.items() if c>=threshold and max(link[0], link[1])<max_dim]
        print("size of threshold tuples:", len(threshold_tuples))
        row = np.array([k[0] for k, _ in threshold_tuples])
        col = np.array([k[1] for k, _ in threshold_tuples])
        dim = max(max(row), max(col)) + 1
        conns = np.array([c for _, c in threshold_tuples])
        # print(row)
        # print(col)
        # print(conns)
        # print(dim)
        X = bsr_matrix((conns,(row, col)))
        X_np = X.toarray()
        labels = cluster.SpectralClustering(n_clusters = 6).fit_predict(X_np)
        print(f)
        printClusters(labels)
        # print(labels)




window = 1
threshold = 10
max_dim = 2000

with open(d + 'word_index_dict.json', 'r') as infile:
    word_index_dict = json.load(infile)
    index_word_dict = {v: f for f,v in word_index_dict.items()}
# try:
    # wordFreq()
# wordFreq()

# min_count = 200
# interval = (datetime.datetime.now() - datetime.datetime(2017, 7, 1, 0, 0)).days
# # interval = 2
#
# with open(d + 'word_count_dict.json', 'r') as infile:
#     word_count_dict = json.load(infile)
#

#
#
# wordssorted = sorted(word_count_dict.items(), key = lambda x: x[1], reverse=True)
#
# current_count = wordssorted[0][1]
# max_index = 10000
# for w, c in wordssorted:
#     if c<min_count:
#         max_index = word_index_dict[w]
#         break
#
# print("max index:", max_index)
#
# timeWindow(window=1)
# conn2.close()
# except:
#     e = sys.exc_info()[0]
#     print("<p>Error: %s</p>" % e)


clusterMatrix()

