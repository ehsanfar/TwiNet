import matplotlib.pyplot as plt
import matplotlib.patches as patches
from datetime import datetime, timedelta

from os.path import dirname, abspath, isfile, isdir, join
from os import listdir
import re
import pickle
import numpy as np
from collections import defaultdict, Counter


d = dirname(dirname(abspath(__file__))) + '/dataset/'
if isdir("/Users/abbasehsanfar/"):
    dir_Download = "/Users/abbasehsanfar/Downloads/windows1500/"
elif isdir("/Users/ccclab/"):
    dir_Download = "/Users/ccclab/Documents/PROJECTS/DATA/TwiNet/windows1500/"
else:
    dir_Download = d


class Cluster():
    def __init__(self, window, keystr, algorithm):
        self.keystr = keystr
        self.window = window
        self.algorithm = algorithm
        self.wordlist = []
        self.silhouettes = []
        self.totalscore = None
        self.stemmedDict = defaultdict(list)


levels = [1, 3, 7, 21]
height = 100
sample_dict = {1: {"2018-01-03": [10, 6, 3, 1], "2018-01-04": [7, 5, 2]}, 3: {"2018-01-04": [3 , 1]}, 7:{"2018-01-06": [14, 4], "2018-01-13": [7, 4]}, 21: {"2018-01-13": [10, 5]}}
colorlist = ['dimgrey', 'grey', 'darkgrey', 'lightgrey', 'snow']

def plotLevelDic(clusterDict):
    global levels, height, my_dpi
    dylevel = height / len(levels)
    fig, ax = plt.subplots(1, 1, figsize=(15, 6), dpi=my_dpi)

    fistdatelist = [datetime.strptime(min(dic.keys()), '%Y-%m-%d') - timedelta(days=lev//2) for lev, dic in clusterDict.items()]
    lastedatelist = [datetime.strptime(max(dic.keys()), '%Y-%m-%d') + timedelta(days=lev//2) for lev, dic in clusterDict.items()]
    firstdate = max(fistdatelist)
    lastdate = min(lastedatelist)

    for i, lev in enumerate(sorted(levels)):
        levDict = clusterDict[lev]
        time_clusters = sorted(levDict.items())
        dtlist = [datetime.strptime(k, '%Y-%m-%d') for k, _ in time_clusters]
        print(time_clusters)
        windowclusterslist = [sorted([c/sum(cl) for c in cl]) for _, cl in time_clusters]
        # print("normized cluster lists:", windowclusterslist)
        xlist = [(a-firstdate).days - lev//2 for a in dtlist]
        print("xlist:", xlist)
        print("dvlevel:", dylevel)
        for x, clist in zip(xlist, windowclusterslist):
            ylist = [dylevel*i]
            ylist.extend([(i+c)*dylevel for c in np.cumsum(clist[:-1])])
            print("clist and ylist:", clist, ylist)
            cols = colorlist[:len(clist)][::-1]
            for j, (y, w) in enumerate(zip(ylist, clist)):
                print("width and height:", (x,y), lev, w*dylevel)

                ax.add_patch(
                    patches.Rectangle(
                        (x, y),
                        lev,
                         w*dylevel,
                        facecolor = cols[j],
                        edgecolor = 'black',
                        linewidth = 0.5# remove background
                    )
                )

    plt.ylim(0, height)
    plt.xlim(0, (lastdate - firstdate).days)
    plt.xlabel('time')
    dt = 7
    plt.xticks(range(0, (lastdate - firstdate).days, dt), [(firstdate + timedelta(days = i)).strftime("%Y-%m-%d") for i in range(0, (lastdate - firstdate).days, dt)], rotation = 60)
    plt.yticks([(i+0.5)*dylevel for i in range(len(levels))], levels)
    plt.ylabel('granularity')
    # plt.show()

def createClusterDict():
    global windows, percentiles, algorihtms, affexes
#    dir_local = dir_Download + "withpopularityburstiness/"
    dir_local = d
    for percentile in percentiles:
        for algorithm in algorihtms:
            for affex in affexes:
                clusterWordDict = defaultdict(defaultdict)
                clusterScoreDict = defaultdict(defaultdict)
                for win in windows:
                    print("%s%s_percentile%d%s.p"%(algorithm, str(win).zfill(2), percentile, affex))
                    winclustersdict = pickle.load(open(dir_local + "%s%s_percentile%d%s.p"%(algorithm, str(win).zfill(2), percentile, affex), "rb"))
                    tempclusterworddict = defaultdict(list)
                    tempclusterscoredict = defaultdict(list)
                    for keystr, clist in winclustersdict.items():
                        sortedlist = sorted(clist, key = lambda x:x.totalscore/len(x.wordlist),  reverse= True)
                        print("NEW")
                        print([(e.totalscore, e.totalscore/len(e.wordlist)) for e in sortedlist])
                        clusterwordlist = [e.wordlist for e in sortedlist][:4]
                        clusterscorelist = [int(e.totalscore/len(e.wordlist)) for e in sortedlist][:4]

                        print(keystr, "percentile, algo, affex, window:", percentile, algorithm, affex, win)
                        print(clusterwordlist)
                        tempclusterworddict[keystr] = clusterwordlist
                        tempclusterscoredict[keystr] = clusterscorelist

                    clusterWordDict[win] = tempclusterworddict
                    clusterScoreDict[win] = tempclusterscoredict

                yield (clusterScoreDict, clusterWordDict, tuple([percentile, algorithm, affex]))

# my_dpi = 1000
my_dpi = 100
windows = [1, 3, 7, 21]
percentiles = [50, 90, 99]
algorihtms = ['spectral', 'kmeans']
affexes = ['_ind', '_jour', '']
# clusterscoredict, _ = createClusterDict()
# print(clusterscoredict)

for clusterscoredict, _, (perc, algo, affex) in createClusterDict():
    plotLevelDic(clusterscoredict)
    # plt.savefig(d + 'clustermap_perc%s_%s%s.eps' % (perc, algo, affex), format='eps', dpi=my_dpi,
    plt.savefig(d + 'clustermap_perc%s_%s%s.png' % (perc, algo, affex), format='png', dpi=my_dpi, bbox_inches='tight')




