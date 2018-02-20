import numpy as np 
def printall(file, sth): 
	loc = glob = None

	try:
		loc = [k for k,v in file.locals().items() if v is sth][0] 
		glob = [k for k,v in file.globals().items() if v is sth][0] 
	except: 
		pass 

	print('%s: ', glob, sth)
	


def histogramCount(timecountlist, bins):
	timecountlist = sorted(list(timecountlist))
	# print(timecountlist[:10])
	timelist = [e[0] for e in timecountlist]
	maxtime = max(timelist)
	mintime = min(timelist)
	linspaces = np.linspace(mintime, maxtime, bins + 1)
	countlist = []
	i = 0 
	for l in linspaces[1:]:
		tempcount = 0 
		while i< len(timecountlist) and timecountlist[i][0]<=l:
			tempcount += timecountlist[i][1]
			i += 1
			
		countlist.append(tempcount)
	
	print("sum retweeters:", sum(countlist))
	return countlist, list(linspaces)

# y, x = histogramDict([(0,2), (4,5), (6,3), (6.5,6), (9,1)], 10)
# print(y, x)
			
		 
		 
		
	
	