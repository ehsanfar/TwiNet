import pickle
# import cPickle
from os.path import dirname, abspath
import os
import json
import csv

d = dirname(dirname(abspath(__file__))) + '/dataset/'
# filepath = d + "/user_doctorow_retweeters.dat"
# print(filepath)
# print(os.path.getsize(filepath))

import os
# for file in os.listdir(d):
#     if file.endswith(".dat") and 'user' in file:
#         print file
#         filepath = d + '/' + file
#         try:
#             with open(filepath, "r") as f:
#                 # for line in f:
#                 #     print(line)
#                 sample_data = cPickle.load(f)
#                 # dic = {int(k): int(v) for k,v in sample_data.items()}
#                 dic = sorted([(int(k), int(v)) for k,v in sample_data.items()], key = lambda x: x[1], reverse=True)
#                 print(dic)
#                 with open(file[:-4]+'.json', 'w') as outfile:
#                     json.dump(dic, outfile)
#
#         except EOFError:
#             pass

def saveUserIDJson():
    user_id_dict = {}

    with open(d + '/user_id.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(row['id'], row['screen_name'])
            user_id_dict[row['screen_name']] = row['id']

        with open(d + 'user_id_dict.json', 'w') as outfile:
            json.dump(user_id_dict, outfile)


def updateStatueRetweetTime():
	global d
	with open(d + 'status_retweettime_dict.json', 'r') as infile:
		status_rettime_dict = json.load(infile)

	status_rettime_dict = {int(s): t for s, t in status_rettime_dict.items()}
	with open(d + 'status_retweetcount_dict.json', 'r') as infile:
		status_retcount_dict = defaultdict(int, json.load(infile))

	# print(status_retcount_dict)
	status_retcount_dict = {int(s): int(t) for s,t in status_retcount_dict.items()}
	with open(d + 'status_time_dict.json', 'r') as infile:
		status_time_dict = json.load(infile)

	status_time_dict = {int(s): t for s, t in status_time_dict.items()}
	with open(d + 'boundary_probability.json', 'r') as infile:
		boundary_probability = json.load(infile)
	# boundary_prob = {(int(re.search(r'\(\d+),(\d+)\)').group(1)), int(re.search(r'\(\d+),(\d+)\)').group(2))): float(v) for k,v in boundary_probability_dict.items()}
	boundary_probability = [tup for tup in boundary_probability if tup[2]>0]
	deltatimelist = []
	l = len(status_rettime_dict)
	print("length:", l)
	j = 0
	for status, timelist in status_rettime_dict.items():
		j += 1
		count = status_retcount_dict[status]
		deltacount = max(count - len(timelist), 0)
		if deltacount > 0:
			t1 = status_time_dict[status] - 60
			mintime = min(timelist) - t1
			# deltalist = [t - t1 for t in timelist]

			temp_boundaries = [k for k in boundary_probability if k[0]<=mintime]
			probabilities = [k[2] for k in temp_boundaries]
			sumprob = sum(probabilities)
			probabilities = [e/sumprob for e in probabilities]

			# print(mintime, len(temp_boundaries))
			sample_boundaries = np.random.choice(list(range(len(temp_boundaries))), deltacount, p=probabilities)
			# print(mintime, deltacount, sample_boundaries)
			newtimelist = [] # [int(np.random.uniform(b1, b2)) for s in sample_boundaries for b1, b2, _ in temp_boundaries[s]]
			for s in sample_boundaries:
				b1, b2, _ = temp_boundaries[s]
				newtimelist.append(int(np.random.choice(range(b1, b2))+t1))

			print(l ,j , mintime, deltacount)
			status_rettime_dict[status].extend(newtimelist)

		# if count <100 and count>40:
		# 	t1 = status_time_dict[status] - 60
		# 	retlist = status_rettime_dict[status]
		# 	deltalist = [t - t1 for t in retlist]
		# 	deltatimelist.extend(deltalist)

		# elif count > 200:
		# 	t1 = status_time_dict[status]
		# 	retlist = status_rettime_dict[status]
		# 	deltalist = [t - t1 for t in retlist]
		# 	# deltatimelist.extend(deltalist)
		# 	print("min delta time:", min(deltalist))

	with open(d + 'status_retweettime_dict_enriched.json', 'w') as outfile:
		json.dump(status_rettime_dict, outfile)

	# deltatimelist = sorted(deltatimelist)
	# maxdelta = max(deltatimelist)
	# bins = int(maxdelta//300)
	# print("bins:", bins)
	# print(deltatimelist.count(0))
	# # r = np.random.exponential(s, 2000000)
	# # y0, x0 = np.histogram(r, bins = 2000)
	# y1, x1 = np.histogram(deltatimelist, bins = bins)
	# x1 = [int(e) for e in x1]
	# # x1 = [e/3600. for e in x1]
	# boundlist = list(zip(x1[:-1], x1[1:]))
	# # x1 = [(x1[i]+x1[i-1])/2. for i in range(1, len(x1))]
    #
	# # print(boundlist)
    #
    #
	# # print("average x:", sum(deltatimelist)/len(deltatimelist))
	# sumcount = len(deltatimelist)
	# y1 = [e/sumcount for e in y1]
	# # boundlist = ['(%d,%d)'%(tup[0],tup[1]) for tup in boundlist]
	# probability_list  = [tuple([tup[0], tup[1], round(prob,3)]) for tup, prob in zip(boundlist, y1)]
    #
	# with open(d + 'boundary_probability.json', 'w') as outfile:
	# 	json.dump(probability_list, outfile)
    #
	# 	# sample_boundaries = np.random.choice(boundlist, 100, p=y1)
	# 	# print(sample_boundaries)
	# 	#
    #
	# 	#
	# # plt.figure()
	# # plt.plot(x1, y1)
	# # plt.xlim(0,20)
	# # plt.show()
