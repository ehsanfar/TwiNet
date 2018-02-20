import pickle
from os.path import dirname, abspath, isfile
import matplotlib.pyplot as plt
from .generalFuncs import *
import numpy as np 
import json 
from collections import defaultdict
import pandas as pd
from statsmodels.tsa.api import VAR
import pandas as pd
import datetime


import re
from itertools import islice

d = dirname(dirname(abspath(__file__))) + '/dataset/'

def readPickle():
	d = dirname(dirname(abspath(__file__))) + '/clusternameClusterDict.pickle'
	with open(d, 'rb') as outfile:
		clusters = pickle.load(outfile)
	print(len(clusters))
	return clusters

def drawRetweeters(): 
	fd = dirname(dirname(abspath(__file__))) + '/figures/'
	clusters = readPickle()
	clustervalues = list(clusters.values())
	status_length_tuples = [(len(c.statususer_one), len(c.statususer_two)) for c in clustervalues]
	# print(status_length_tuples)
	status_length_sum = [sum(l) for l in zip(*status_length_tuples)]
	# print(status_length_sum)
	retweeter_length_tuple = [(len(c.retweeters_one), len(c.retweeters_two)) for c in clustervalues]
	retweeter_length_sum = [sum(l) for l in zip(*retweeter_length_tuple)]


	plt.figure()
	ensamble_list = list(zip(status_length_tuples, retweeter_length_tuple))
	#filetr
	ensamble_list = [(tup1, tup2) for tup1, tup2 in ensamble_list if (tup1[0]>10 and tup1[1]>10)]
	#normize by number of statuses
	normalized_retweeters = [(tup1, (tup2[0]/tup1[0], tup2[1]/tup1[1])) for tup1, tup2 in ensamble_list]
	x1y1 = [(tup1[0], tup2[0]) for tup1, tup2 in normalized_retweeters]
	x2y2 = [(tup1[1], tup2[1]) for tup1, tup2 in normalized_retweeters]
	x1y2 = [(tup1[0], tup2[1]) for tup1, tup2 in normalized_retweeters]
	x2y1 = [(tup1[1], tup2[0]) for tup1, tup2 in normalized_retweeters]


	plt.figure()
	plt.scatter(*zip(*x1y1))
	plt.title("user set %d status vs set %d retweet"%(1, 1))
	plt.xlabel('cluster status frequency')
	plt.ylabel('cluster retweet frequency')
	plt.legend(['clusters'])
	plt.savefig(fd + "status_retweet_11.pdf", bbox_inches='tight')
	# plt.show()

	plt.figure()
	plt.scatter(*zip(*x2y1))
	plt.title("user set %d status vs set %d retweet"%(2, 1))
	plt.xlabel('cluster status frequency')
	plt.ylabel('cluster retweet frequency')
	plt.legend(['clusters'])
	plt.savefig(fd + "status_retweet_21.pdf", bbox_inches='tight')
	# plt.show()

	plt.figure()
	plt.scatter(*zip(*x1y2))
	plt.title("user set %d status vs set %d retweet"%(1, 2))
	plt.savefig(fd + "status_retweet_12.pdf", bbox_inches='tight')
	# plt.show()

	plt.figure()
	plt.scatter(*zip(*x2y2))
	plt.title("user set %d status vs set %d retweet"%(2, 2))
	plt.xlabel('cluster status frequency')
	plt.ylabel('cluster retweet frequency')
	plt.legend(['clusters'])
	plt.savefig(fd + "status_retweet_22.pdf", bbox_inches='tight')
	plt.show()

	# unique_retweeter_length_tuple = [(len(set(c.retweeters_one)), len(set(c.retweeters_two))) for c in clustervalues]
	# unique_retweeter_length_sum = [sum(l) for l in zip(*unique_retweeter_length_tuple)]
	# # print(len(retweeter_length_tuple), len(status_length_tuples))
	# # print(retweeter_length_tuple)
	# print('draw retweeters', retweeter_length_sum)
	# print('draw retweeters', unique_retweeter_length_sum)



	# average_retweets_list = list(zip(status_length_tuples, retweeter_length_tuple))
	# # print(average)
	# average_retweets = [(tup2[0]/(tup1[0]), tup2[1]/(tup1[1])) for tup1, tup2 in zip(status_length_tuples, retweeter_length_tuple) if (tup1[0]>0 and tup1[1]>0)]
	# average_retweets = [tup for tup in average_retweets if sum(tup)>0]
	# print('length of average retweets: ', len(average_retweets))
	# # print(average_retweets[:100])
	# a, b = list(zip(*average_retweets))
	# average_retweets_sum = (sum(a)/len(average_retweets), sum(b)/len(average_retweets))
	# print(average_retweets_sum)

def drawStatusRetweetDistribution(): 
	fd = dirname(dirname(abspath(__file__))) + '/figures/'
	d = dirname(dirname(abspath(__file__))) + '/dataset/'
	clusters = readPickle()
	clusterlist = list(clusters.values())
	with open(d + 'status_time_dict.json', 'r') as infile:
		status_time_dict = json.load(infile)

	status_time_dict = {int(s): t for s,t in status_time_dict.items()}
	with open(d + 'status_retweeter_dict.json', 'r') as infile:
		status_retweeter_dict = json.load(infile)

	status_retweeter_dict = {int(s): r for s,r in status_retweeter_dict.items()}
	status_retweeter_dict = defaultdict(list, status_retweeter_dict)
	sortedclusters = sorted(clusterlist, key = lambda c: len(c.statususer_one) + len(c.statususer_one), reverse = True)
	sampleclusters = [s for s in sortedclusters if len(s.statususer_one)>10 and len(s.statususer_two)>5 and len(s.retweeters_one)+len(s.retweeters_two)>5000]
	for cluster in sampleclusters[:5]:
		try:
			status_one = cluster.statususer_one.keys()
			status_two = cluster.statususer_two.keys()

			time_one = sorted([status_time_dict[s] for s in status_one])
			time_two = sorted([status_time_dict[s] for s in status_two])

			y1, x1 = np.histogram(time_one, bins = 72)
			y2, x2 = np.histogram(time_two, bins = 72)

			print("sum status:", sum(y1) + sum(y2))

			x1 = [(x1[i]+x1[i-1])/2. for i in range(1, len(x1))]
			x2 = [(x2[i]+x2[i-1])/2. for i in range(1, len(x2))]

			x1 = [e%x1[0]/3600. for e in x1]
			x2 = [e%x2[0]/3600. for e in x2]

			plt.figure()
			plt.title("cluster: %s"%cluster.id)
			plt.plot(x1, y1)
			plt.plot(x2, y2)
			plt.xlabel("time steps (seconds)")
			plt.ylabel("status activity (tweets)")
			plt.legend(['status one', 'status two'])
			plt.savefig(fd + 'statuses_cluster_%s.pdf'%cluster.id, bbox_inches='tight')
		except:
			print("exception")
			pass

	# plt.show()

	for cluster in sampleclusters[:5]:
		try:
			status_one = cluster.statususer_one.keys()
			status_two = cluster.statususer_two.keys()

			retweet_one = sorted([len(status_retweeter_dict[s]) for s in status_one])
			retweet_two = sorted([len(status_retweeter_dict[s]) for s in status_two])

			print("length of retweeters one: ", sum(retweet_one), len(cluster.retweeters_one))
			print("length of retweeters two: ", sum(retweet_two), len(cluster.retweeters_two))

			time_one = sorted([status_time_dict[s] for s in status_one])
			time_two = sorted([status_time_dict[s] for s in status_two])

			y1, x1 = histogramCount(zip(time_one, retweet_one), bins = 72)
			y2, x2 = histogramCount(zip(time_two, retweet_two), bins = 72)

			x1 = [(x1[i]+x1[i-1])/2. for i in range(1, len(x1))]
			x2 = [(x2[i]+x2[i-1])/2. for i in range(1, len(x2))]

			x1 = [e%x1[0]/3600. for e in x1]
			x2 = [e%x2[0]/3600. for e in x2]

			plt.figure()
			plt.title("cluster: %s"%cluster.id)
			plt.plot(x1, y1)
			plt.plot(x2, y2)
			plt.xlabel("time steps (seconds)")
			plt.ylabel("retweeter activity (tweets)")
			plt.legend(['retweet one', 'retweet two'])
			plt.savefig(fd + 'retweeter_cluster_%s.pdf'%cluster.id, bbox_inches='tight')

		except:
			print("exception")
			pass

	# plt.show()
		# time_status_one = sorted([(status_time_dict[s], s) for s in status_one])
		# time_status_two = sorted([(status_time_dict[s], s) for s in status_two])



def drawRetweetTimeDist():
	fd = dirname(dirname(abspath(__file__))) + '/figures/'
	global d
	clusters = readPickle()
	clusterlist = list(clusters.values())
	with open(d + 'status_time_dict.json', 'r') as infile:
		status_time_dict = json.load(infile)

	status_time_dict = {int(s): t for s, t in status_time_dict.items()}

	# with open(d + 'status_retweeter_dict.json', 'r') as infile:
	# 	status_retweet_dict = json.load(infile)
    #
	# status_retweet_dict = {int(s): r for s, r in status_retweet_dict.items()}

	with open(d + 'status_retweettime_dict_enriched.json', 'r') as infile:
		status_retweettime_dict = json.load(infile)

	status_retweettime_dict = defaultdict(list, {int(s): r for s, r in status_retweettime_dict.items()})

	sortedclusters = sorted(clusterlist, key=lambda c: len(c.statususer_one) + len(c.statususer_one),
							reverse=True)
	sampleclusters = [s for s in sortedclusters if
					  len(s.statususer_one) > 10 and len(s.statususer_two) > 5 and len(
						  s.retweeters_one) + len(s.retweeters_two) > 5000]

	# for cluster in sampleclusters[:5]:
	# 	try:
	# 		status_one = cluster.statususer_one.keys()
	# 		status_two = cluster.statususer_two.keys()
    #
	# 		retweet_one = sorted([len(status_retweeter_dict[s]) for s in status_one])
	# 		retweet_two = sorted([len(status_retweeter_dict[s]) for s in status_two])
    #
	# 		print("length of retweeters one: ", sum(retweet_one), len(cluster.retweeters_one))
	# 		print("length of retweeters two: ", sum(retweet_two), len(cluster.retweeters_two))
    #
	# 		time_one = sorted([status_time_dict[s] for s in status_one])
	# 		time_two = sorted([status_time_dict[s] for s in status_two])
    #
	# 		y1, x1 = histogramCount(zip(time_one, retweet_one), bins=72)
	# 		y2, x2 = histogramCount(zip(time_two, retweet_two), bins=72)
    #
	# 		x1 = [(x1[i] + x1[i - 1]) / 2. for i in range(1, len(x1))]
	# 		x2 = [(x2[i] + x2[i - 1]) / 2. for i in range(1, len(x2))]
    #
	# 		x1 = [e % x1[0] / 3600. for e in x1]
	# 		x2 = [e % x2[0] / 3600. for e in x2]
    #
	# 		plt.figure()
	# 		plt.title("cluster: %s" % cluster.id)
	# 		plt.plot(x1, y1)
	# 		plt.plot(x2, y2)
	# 		plt.xlabel("time steps (seconds)")
	# 		plt.ylabel("retweeter activity (tweets)")
	# 		plt.legend(['retweet one', 'retweet two'])
	# 		plt.savefig(fd + 'retweeter_cluster_%s.pdf' % cluster.id, bbox_inches='tight')
    #
	# 	except:
	# 		print("exception")
	# 		pass
	granger_dict = defaultdict(int)
	grangerset_dict = defaultdict(int)
	for cluster in sampleclusters:
		# try:
		print(cluster.id)
		avg1 = avg2 = avg3 = avg4 = 1
		status_one = cluster.statususer_one.keys()
		status_two = cluster.statususer_two.keys()

		time_one = sorted([status_time_dict[s]-60 for s in status_one])
		time_two = sorted([status_time_dict[s]-60 for s in status_two])

		y1, x1 = np.histogram(time_one, bins=72)
		y2, x2 = np.histogram(time_two, bins=72)

		print("sum status:", sum(y1) + sum(y2))

		x1 = [(x1[i] + x1[i - 1]) / 2. for i in range(1, len(x1))]
		x2 = [(x2[i] + x2[i - 1]) / 2. for i in range(1, len(x2))]

		# x1 = [e % x1[0] / 3600. for e in x1]
		# x2 = [e % x2[0] / 3600. for e in x2]

		# avg1 = sum(y1) / float(len(y1))
		# avg2 = sum(y2) / float(len(y2))
		y1 = [e / avg1 for e in y1]
		y2 = [e / avg2 for e in y2]

		t1 = min([status_time_dict[s] for s in status_one])
		t2 = min([status_time_dict[s] for s in status_two])

		retweet_one = [status_retweettime_dict[s] for s in status_one]
		retweet_two = [status_retweettime_dict[s] for s in status_two]
		# print(retweet_one)

		retweet_one = [[r for r in l if r-t1 <= 72*3600] for l in retweet_one]
		retweet_two = [[r for r in l if r-t2 <= 72*3600] for l in retweet_two]


		retweet_one = [e for l in retweet_one for e in l]
		retweet_two = [e for l in retweet_two for e in l]

		print("length of retweeters one: ", len(retweet_one), len(cluster.retweeters_one))
		print("length of retweeters two: ", len(retweet_two), len(cluster.retweeters_two))

		y3, x3 = np.histogram(retweet_one, bins=72)
		y4, x4 = np.histogram(retweet_two, bins=72)

		# time_one = sorted([status_time_dict[s] for s in status_one])
		# time_two = sorted([status_time_dict[s] for s in status_two])

		# y1, x1 = histogramCount(zip(time_one, retweet_one), bins=72)
		# y2, x2 = histogramCount(zip(time_two, retweet_two), bins=72)

		x3 =[(x3[i] + x3[i - 1]) / 2. for i in range(1, len(x3))]
		x4 =[(x4[i] + x4[i - 1]) / 2. for i in range(1, len(x4))]

		# x3 =[e % x3[0] / 3600. for e in x3]
		# x4 =[e % x4[0] / 3600. for e in x4]

		# avg3 = sum(y3) / float(len(y3))
		# avg4 = sum(y4) / float(len(y4))

		y3 = [e/avg3 for e in y3]
		y4 = [e/avg4 for e in y4]

		# print(len(y1), len(y2), len(y3), len(y4))
		keys = ['status_one', 'status_two', 'retweet_one', 'retweet_two']
		df = pd.DataFrame(np.column_stack([y1, y2, y3, y4]), columns=keys)
		seconds = [sum(tup)/float(len(tup)) for tup in zip(x1, x2, x3, x4)]
		df.index = [datetime.datetime.fromtimestamp(e) for e in seconds]

		model = VAR(df)
		results = model.fit(2)
		r1 = results.test_causality('status_one', ['retweet_one'], kind='f')
		r2 = results.test_causality('status_one', ['retweet_two'], kind='f')
		r3 = results.test_causality('status_one', ['status_two'], kind='f')
		r4 = results.test_causality('status_two', ['retweet_one'], kind='f')
		r5 = results.test_causality('status_two', ['retweet_two'], kind='f')
		r6 = results.test_causality('status_two', ['status_one'], kind='f')
		r7 = results.test_causality('retweet_one', ['status_one'], kind='f')
		r8 = results.test_causality('retweet_two', ['status_two'], kind='f')

		# print(r1['pvalue'], r2['pvalue'],r3['pvalue'],r4['pvalue'],r5['pvalue'],r6['pvalue'])
		tempdict = defaultdict(int)
		tempdict['r1s1'] += 1 if r1['pvalue'] <= 0.05 else 0
		tempdict['r2s1'] += 1 if r2['pvalue'] <= 0.05 else 0
		tempdict['s2s1'] += 1 if r3['pvalue'] <= 0.05 else 0
		tempdict['r1s2'] += 1 if r4['pvalue'] <= 0.05 else 0
		tempdict['r2s2'] += 1 if r5['pvalue'] <= 0.05 else 0
		tempdict['s1s2'] += 1 if r6['pvalue'] <= 0.05 else 0
		tempdict['s1r1'] += 1 if r7['pvalue'] <= 0.05 else 0
		tempdict['s2r2'] += 1 if r8['pvalue'] <= 0.05 else 0

		for k, v in tempdict.items():
			granger_dict[k] += v

		keys = ['r1s1', 'r2s1', 's2s1', 'r1s2', 'r2s2', 's1s2', 's1r1', 's2r2']
		namelist = [k for k in keys if tempdict[k] == 1]

		grangerset_dict[tuple(namelist)] += 1


	print(granger_dict)
	print(grangerset_dict)



		# r1 = results.test_causality('status_one', ['retweet_one', 'status_two', 'retweet_two'], kind='f')
		# r1 = results.test_causality('status_two', ['retweet_one', 'status_one', 'retweet_two'], kind='f')




		# results = model.fit(2)
		# # plt.figure()
		# results.plot()
		# print(results.summary())

	# plt.show()
		# df.index = pd.DatetimeIndex([sum(tup)/float(len(tup)) for tup in zip(x1, x2, x3, x4)])

		# print(df.head)
		# plt.figure()
		# plt.title("cluster: %s" % cluster.id)
		# plt.plot(x1, y1)
		# plt.plot(x2, y2)
		# plt.plot(x3, y3)
		# plt.plot(x4, y4)
		# plt.xlim(0, 72)
		# plt.xlabel("time steps (seconds)")
		# plt.ylabel("retweet time (tweets)")
		# plt.legend(['status_one', 'status_two', 'retweet one', 'retweet two'])
		# plt.savefig(fd + 'statusretweettime_cluster_%s.pdf' % cluster.id, bbox_inches='tight')
        # #
		# # except:
		# # 	print("exception")
		# # 	pass

						# testRetweetDistribution()