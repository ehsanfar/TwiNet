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
# import time, datetime
import re
import csv
import statistics
from itertools import islice, product

from scipy.sparse import bsr_matrix

import sklearn.cluster as cluster
from sklearn.metrics import silhouette_samples, silhouette_score

d = dirname(dirname(abspath(__file__))) + '/dataset/'
if isdir("/Users/abbasehsanfar/"):
    dir_Download = "/Users/abbasehsanfar/Downloads/windows1500/"
elif isdir("/Users/ccclab/"):
    dir_Download = "/Users/ccclab/Documents/PROJECTS/DATA/TwiNet/windows1500/"
else:
    dir_Download = d



conn2 = pymysql.connect(host='localhost',
                           user='root',
                           password='1574',
                           db='twitternetwork',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

class Cluster():
    def __init__(self, window, keystr, algorithm):
        self.keystr = keystr
        self.window = window
        self.algorithm = algorithm
        self.wordlist = []
        self.silhouettes = []
        self.totalscore = None
        self.stemmedDict = defaultdict(list)


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
        self.linkpopularity = defaultdict(float)
        self.linkburstiness = defaultdict(float)

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
    global conn2, d, interval
    sql_read = "Select created_at, user_id, tokens from statustokens where created_at> now()- interval %d day order by created_at"%interval
    # time = defaultdict(int)
    window_word_count_dict = defaultdict(Counter)
    bin = 0
    first = True
    firsttime = None
    winDict_ind = {}
    winDict_jour = {}

    userset_ind = set([])
    userset_jour = set([])
    with open(d + 'result_prob_est.csv', 'r') as infile:
        csvrows = csv.DictReader(infile)
        for row in csvrows:
            if int(float(row['est'])) == 1:
                userset_jour.add(int(row['user_id']))
            else:
                userset_ind.add(int(row['user_id']))

    with conn2.cursor() as cursor2:
        cursor2.execute(sql_read)
        result = cursor2.fetchall()
        # for i, r in list(enumerate(result))[:1000]:
        stored_keystr = None
        for i, r in list(enumerate(result)):
            user_id = int(r['user_id'])
            if first:
                firsttime = r['created_at'].replace(hour = 0, minute = 0, second=0, microsecond=0)
                # winstateObj = WindowState(firsttime, window)
                stored_keystr = (firsttime + timedelta(days = window*((r['created_at'] - firsttime).days//window) + window//2)).strftime("%Y-%m-%d")
                first = False

            t = r['created_at']
            dt = t - firsttime
            keytime = firsttime + timedelta(days = window*(dt.days//window) + window//2)
            keystr = keytime.strftime("%Y-%m-%d")
            if keystr in winDict_ind and user_id in userset_ind:
                winObj = winDict_ind[keystr]
            elif keystr in winDict_jour and user_id in userset_jour:
                winObj = winDict_jour[keystr]
            else:
                starttime = firsttime + timedelta(days = window*(dt.days//window))
                winObj = Window(window, starttime = starttime, keystr = keystr)
                if user_id in userset_ind:
                    winDict_ind[keystr] = winObj
                else:
                    winDict_jour[keystr] = winObj
            # print("time and keytime and window:", t, dt.days, window, window*(dt.days//window) + window//2, keystr)
            # if keytime in window_word_count_dict:

            winObj.addTweet(text = r['tokens'])
            if i %1000 == 0:
                print(i, r['tokens'])
            # else:
            #     window_word_count_dict[keytime] = word_count_dict
            # print(type(t), t, t + timedelta(days = 3))

            # time_str = datetime.datetime.strftime("%Y-%m-%d")
            if keystr != stored_keystr:
                pickle.dump(winDict_jour[stored_keystr], open(dir_Download + "window%s_%s_ind.p" % (str(window).zfill(2), str(stored_keystr)), "wb"))
                pickle.dump(winDict_ind[stored_keystr], open(dir_Download + "window%s_%s_ind.p" % (str(window).zfill(2), str(stored_keystr)), "wb"))
                stored_keystr = keystr
    # print(winstateObj.wordcounterdict)
    # print(winstateObj.wordconnections)
    # print(sorted([(k, w.tweetcount) for k, w in winDict.items()]))
    # print([(k, sum(w.wordcounter.values())) for k, w in sorted(winDict.items())])

    # print(window_word_count_dict)
    # with open(d + 'window_word_count_dict_w=%d.json'%window, 'w') as outfile:
    #     json.dump(window_word_count_dict, outfile)
    print("download directory name:", dir_Download)
    for keystr, winObj in winDict_ind.items():
        pickle.dump(winObj, open(dir_Download + "window%s_%s_ind.p"%(str(window).zfill(2), str(keystr)), "wb"))
    for keystr, winObj in winDict_jour.items():
        pickle.dump(winObj, open(dir_Download + "window%s_%s_jour.p"%(str(window).zfill(2), str(keystr)), "wb"))
    # pickle.dump(winstateObj, open(dir_Download + "windowStateObj_win%s.p"%str(window), "wb"))


def updateLinkCountBurstiness():
    global window
    for affex in ['_jour', '_ind']:
        pattern = re.compile(r"window%s\_[\d]+-[\d]+-[\d]+%s\.p"%(str(1).zfill(2), affex))
        filenames = [f for f in listdir(dir_Download) if isfile(join(dir_Download, f)) and pattern.match(f)]
        link_count_dict = Counter()
        link_list_dict = defaultdict(list)
        link_avg_dict = defaultdict(float)
        link_std_dict = defaultdict(float)
        win_linkpopularity_dict = defaultdict(dict)
        win_linkcount_dict = defaultdict(Counter)
        win_tweetcount_dict = Counter()
        winDict = {}
        win_num = len(filenames)//window
        sortedfiles = sorted(filenames, reverse= True)
        for w in range(win_num):
            # datetimelist = []
            # print(sortedfiles[w*window: w*window + window])
            rekeylist = sorted([re.search(r'.+_(\d+\-\d+\-\d+)%s\.p'%affex, f).group(1) for f in sortedfiles[w*window: (w + 1)*window]])
            print(rekeylist)
            datetimelist = [datetime.strptime(e, '%Y-%m-%d') for e in rekeylist]
            dt = datetimelist[-1] - datetimelist[0]
            win_datetime = datetimelist[0] + dt/2
            win_keystr = win_datetime.strftime("%Y-%m-%d")
            starttime = datetimelist[0]
            winObj = Window(window, starttime, win_keystr)
            for f in sortedfiles[w*window: w*window + window]:
                tempwindow = pickle.load(open(dir_Download + f, "rb"))
                win_linkcount_dict[win_keystr] = tempwindow.connectioncounter
                win_tweetcount_dict[win_keystr] += tempwindow.tweetcount

            winObj.connectioncounter = win_linkcount_dict[win_keystr]
            winObj.tweetcount = win_tweetcount_dict[win_keystr]
            winDict[win_keystr] = winObj
        # for f in sorted(filenames):
        #     print("read:",f)
        #     tempwindow = pickle.load(open(dir_Download + f, "rb"))
        #     link_count_dict += tempwindow.connectioncounter
        #     win_linkcount_dict[tempwindow.keystr] = tempwindow.connectioncounter
        #     win_tweetcount_dict[tempwindow.keystr] +=  tempwindow.tweetcount
        #     winDict[tempwindow.keystr] = tempwindow

        tweetcount = sum(win_tweetcount_dict.values())

        for keystr, counter in sorted(win_linkcount_dict.items()):
            print("popularity:", keystr)
            win_linkpopularity_dict[keystr] = {link: tweetcount*(0.5 + win_linkcount_dict[keystr][link])/(0.5 + win_tweetcount_dict[keystr]) for link in counter}
            for link, pop in win_linkpopularity_dict[keystr]:
                link_list_dict[link].append(pop)

        for link, li in link_list_dict.items():
            link_avg_dict[link] = sum(li)/len(li)
            link_std_dict[link] = np.std(li)

        for keystr, winObj in sorted(winDict.items()):
            print("update objects:", keystr)

            winObj.linkburstiness = {link: (pop-link_avg_dict[link] + 0.5)/(0.5 + link_std_dict[link]) for link, pop in win_linkpopularity_dict[keystr].items()}
            winObj.linkpopularity = win_linkpopularity_dict[keystr]

        with open(d + 'link_count_dict%s.json'%affex, 'w') as outfile:
            json.dump({json.dumps(k): c for k, c in link_count_dict.items()}, outfile)

        # print("link average popularity:", sorted(link_avg_dict.values(), reverse= True)[:100])
        # print("link std popularity:", sorted(link_std_dict.values(), reverse= True)[:100])
        for keystr, winObj in winDict.items():
            pickle.dump(winObj, open(dir_Download + "withpopularityburstiness/window%s_%s%s.p"%(str(window).zfill(2), str(keystr), affex), "wb"))

    # updateLinkRankingScore()



def updateLinkRankingScore(alpha = 0.5):
    global dir_Download, window
    beta = 1-alpha
    for affex in ['_jour', '_ind']:
        pattern = re.compile(r"window%s\_\d+\-\d+\-\d+%s\.p"%(str(window).zfill(2), affex))
        dir_local = dir_Download + "withpopularityburstiness/"
        filenames = [f for f in listdir(dir_local) if isfile(join(dir_local, f)) and pattern.match(f)]
        print("filenames: ", )
        win_score_dict = defaultdict(defaultdict)
        for f in sorted(filenames):
            print(f)
            tempwindow = pickle.load(open(dir_local + f, "rb"))
            keystr = tempwindow.keystr
            counter1 = tempwindow.linkpopularity
            counter2 = tempwindow.linkburstiness
            scoreDict = {t1[0]: int(alpha * t1[1] + beta * t2[1]) for t1, t2 in zip(counter1.items(), counter2.items())}
            win_score_dict[keystr] = scoreDict
            # print(len(win_score_dict), len(scoreDict))
            # print(Counter(scoreDict.values()))

        print([sum(e[1].values())/len(e[1].values()) for e in win_score_dict.items()])
        pickle.dump(win_score_dict, open(dir_local + "windows%s_linkscore_dict%s.p"%(str(window).zfill(2), affex), "wb"))


def printClusters(labels):
    global index_word_dict
    counter = Counter(labels)
    cluster_word_dict = defaultdict(list)
    for i, l in enumerate(labels):
        if counter[l]< 10:
            cluster_word_dict[l].append(index_word_dict[i])

    for cluster, words in cluster_word_dict.items():
        print(cluster, words)


def clusterMatrix(algorithm = 'spectral'):
    global window, max_dim, dir_Download, score_percentile, index_word_dict, stem_memory_dict, dir_local, win_score_dict, affex
    # pattern = re.compile(r"window%s.+\.p"%str(window).zfill(2))
    win_cluster_dict = defaultdict(list)
    for datestr, scoredict in sorted(win_score_dict.items()):
        scoreslist = list(scoredict.values())
        # print(scoreslist)
        threshold = np.percentile(scoreslist, score_percentile)
        print("size of tuples;", len(scoreslist), " and threshold:", threshold, " and score_percentile:", score_percentile)
        threshold_tuples = [(link, 1) for link, c in scoredict.items() if c>threshold and max(link[0], link[1])<max_dim]
        print("size of threshold tuples:", len(threshold_tuples))
        if not threshold_tuples:
            continue
        row = np.array([k[0] for k, _ in threshold_tuples])
        col = np.array([k[1] for k, _ in threshold_tuples])
        dim = max(max(row), max(col)) + 1
        if dim <= 10:
            continue
        conns = np.array([c for _, c in threshold_tuples])
        # print(row)
        # print(col)
        # print(conns)
        # print(dim)
        X = bsr_matrix((conns,(row, col)))
        X_np = X.toarray()
        # print(f)
        for n in range(20, 21):
            try:
                if algorithm == 'spectral':
                    print("spectral")
                    labels = cluster.SpectralClustering(n_clusters = n).fit_predict(X_np)
                elif algorithm == 'kmeans':
                    print("kmeans")
                    labels = cluster.KMeans(n_clusters=n).fit_predict(X_np)
            except:
                continue
            sample_silhouette_values = silhouette_samples(X_np, labels)
            # silhouette_avg = silhouette_score(X_np, labels)
            cluster_word_dict = defaultdict(list)
            cluster_index_dict = defaultdict(list)
            cluster_link_dict = defaultdict(list)
            cluster_score_dict = defaultdict(int)
            cluster_silhouette_dict = defaultdict(list)
            counter = Counter(labels)

            for i, l in enumerate(labels):
                if counter[l] < 50:
                    cluster_word_dict[l].append(index_word_dict[i])
                    cluster_index_dict[l].append(i)

            for cl, index_list in cluster_index_dict.items():
                totalscore = 0
                sortedindices = sorted(index_list)
                cluster_silhouette_dict[cl] = sample_silhouette_values[labels == cl]
                for i, w1 in enumerate(sortedindices):
                    for j, w2 in enumerate(sortedindices[i:]):
                        tup = tuple(sorted([w1, w2]))
                        if tup in scoredict:
                            totalscore += scoredict[tup]
                            cluster_link_dict[cl].append(tup)

                cluster_score_dict[cl] = totalscore
                # print(len(cluster_index_dict[cl]), len(cluster_silhouette_dict[cl]))
                # avg_silhouette = sum(cluster_silhouette_dict[cl]) / len(cluster_silhouette_dict[cl])
                # cluster_silhouette_dict[cl] = avg_silhouette


            # for j in range(n):
                # length = len(sample_silhouette_values[labels == j])
                length = len(cluster_index_dict[cl])
                if length < 15 and length > 1:
                    print("Cluster:", cl, " with number of words and links:", len(cluster_index_dict[cl]),
                          len(cluster_link_dict[cl]),
                          " and score:", cluster_score_dict[cl], " and silhouette score:", cluster_silhouette_dict[cl])
                    avg_silhouette = sum(cluster_silhouette_dict[cl]) / len(cluster_silhouette_dict[cl])
                    clusterObj = Cluster(window=window, keystr=datestr, algorithm=algorithm)
                    clusterObj.silhouettes = avg_silhouette
                    clusterObj.wordlist = cluster_word_dict[cl]
                    clusterObj.stemmedDict = {word: stem_memory_dict[word] if word in stem_memory_dict else [word] for word in clusterObj.wordlist}
                    clusterObj.totalscore = cluster_score_dict[cl]
                    win_cluster_dict[datestr].append(clusterObj)
                    # print("For n_clusters =", n, " cluster =", j, ", The average silhouette_score:", avg_silhouette, " size:", length)

        print("The nubmer of cluster in:", datestr, " is : ", len(win_cluster_dict[datestr]))
        # printClusters(labels)
        # print(labels)

    pickle.dump(win_cluster_dict, open(dir_local + "%s%s_percentile%s%s.p" % (algorithm, str(window).zfill(2),str(score_percentile).zfill(2), affex), "wb"))
    # print("sum of popularity and burstiness:", sum(popularity_list), sum(burstienss_list))


windows = [1, 3, 7, 21]
# windows = [21]
# window = 1
threshold = 2
max_dim = 2000
score_percentile_list = [50, 90, 99]

with open(d + 'stem_memory.json', 'r') as infile:
    stem_memory_dict = json.load(infile)

with open(d + 'word_index_dict.json', 'r') as infile:
    word_index_dict = json.load(infile)
    index_word_dict = {v: f for f,v in word_index_dict.items()}
# try:
    # wordFreq()
# wordFreq()

min_count = 200
interval = (datetime.now() - datetime(2017, 7, 1, 0, 0)).days
# interval = 2


with open(d + 'word_count_dict.json', 'r') as infile:
    word_count_dict = json.load(infile)


wordssorted = sorted(word_count_dict.items(), key = lambda x: x[1], reverse=True)

current_count = wordssorted[0][1]
max_index = 10000
for w, c in wordssorted:
    if c<min_count:
        max_index = word_index_dict[w]
        break
# #
# print("max index:", max_index)
# #
# timeWindow(window=1)
# conn2.close()
# # except:
# #     e = sys.exc_info()[0]
# #     print("<p>Error: %s</p>" % e)
# for window in windows:
#     # updateLinkCountBurstiness()
#     updateLinkRankingScore()
#
# # updateLinkRankingScore()
for window in reversed(windows):
    print("New window: ", window)
    for affex in ['_jour', '_ind', '']:
        dir_local = dir_Download + "withpopularityburstiness/"
        win_score_dict = pickle.load(open(dir_local + "windows%s_linkscore_dict%s.p"%(str(window).zfill(2), affex), "rb"))
        for score_percentile in score_percentile_list:
            print("New percentile:", score_percentile)
            # algorithm = 'spectral'
            for algo in ['spectral']:
                f = "%s%s_percentile%s%s.p"%(algo, str(window).zfill(2),str(score_percentile).zfill(2), affex)
                if isfile(dir_local + f):
                    continue
                print("%s%s_percentile%s%s.p"%(algo, str(window).zfill(2),str(score_percentile).zfill(2), affex))
                clusterMatrix(algorithm = algo)

conn2.close()

