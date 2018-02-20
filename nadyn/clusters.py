from collections import defaultdict 

class Cluster():
    def __init__(self, id, userset_one, userset_two):
        self.id = id
        self.userset_one = userset_one
        self.userset_two = userset_two
        self.statususer_one = {}
        self.statususer_two = {}
        self.userretweeters_one = defaultdict(list)
        self.userretweeters_two = defaultdict(list)
        self.statustimedict = defaultdict(int)
        self.addedretweeters = 0 
        self.reallyaddedretweeters = 0 
        self.retweeters_one = []
        self.retweeters_two = []


    def addStatus(self, status, user, t):
        if user in self.userset_one:
            # print('1')
            self.statususer_one[status] = user
        elif user in self.userset_two:
            # print('2')
            self.statususer_two[status] = user

        self.statustimedict[status] = t
        
        # else:
        #     print("Cluster: user set not found")
            # print('No user category found in cluster: ', self.id)
    
    def addRetweeters(self, status, retweeters):
        self.addedretweeters += len(retweeters) 
        if status in self.statususer_one: 
            # print("status and length of retweeters:", status, len(retweeters))
            self.userretweeters_one[self.statususer_one[status]].extend(retweeters)
            self.retweeters_one.extend(retweeters)
        elif status in self.statususer_two: 
            # print("status and length of retweeters:", status, len(retweeters))
            self.userretweeters_two[self.statususer_two[status]].extend(retweeters)
            self.retweeters_two.extend(retweeters)
    
        
            


