import socket
print(socket.gethostbyname("localhost"))
import argparse
import itertools
import logging
import sys
import re
import os
sys.path.append(os.path.abspath('..'))

from  nadyn.clusters import Cluster
from nadyn.createclusters import *
from nadyn.drawRetweeters import * 
import json
from nadyn.classify_users import *
from nadyn.createNetwork import *


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="This program runs on tweeter experiment.")
    # parser.add_argument('experiment', type=str, nargs='+',
    #                     help='the experiment to run: masv or bvc')
    parser.add_argument('-d', '--numTurns', type=int, default=24,
                        help='simulation duration (number of turns)')
    args = parser.parse_args()
    
    # createUseridtypefiles()
    # classifyUserRetweeterMethod()

    # createClusters()
    # createClusters()	
    # raedPickle()
    # drawRetweeters()
    # drawRetweetTimeDist()

    createNetwork()
    
        
    

