#!/usr/bin/python3

import json
import glob
import os
import sys
import pandas as pd
import hadoop.conf.parser as parser
import hadoop.tasks.stats as stats
import numpy as np
from colorama import Fore, Back, Style

#path = "/mnt/sda/asup-trace/09-19-2018/" 
path = "/mnt/sda/asup-trace/08-18-2018/job_1534545750476_3564"


#if len(sys.argv) >= 2:
#   path = sys.argv[2]
#
#days_dirs =  [(path + name) for name in os.listdir(path) if os.path.isdir(path + name)]
#jobs_history_dir = []
#for path2 in days_dirs:
#    jobs_history_dir.extend([(path2 +'/' + name) for name in os.listdir(path2) if os.path.isdir(path2+ '/' + name)])
#    break; # only for one day
#
#jobs_history_dir.sort()
#print("Number of Jobs:" + str(len(jobs_history_dir)));

jobs_history_dir = [path, '/mnt/sda/asup-trace/08-12-2018/job_1531656020138_354417', '/mnt/sda/asup-trace/08-12-2018/job_1531656020138_353795']
data = []
i = 0
for p in jobs_history_dir:
    try:
        print('\n\n\n')
        with open(p+'/job_counters.json', 'r') as jcfd, open(p+'/conf.json', 'r') as cfd:
            jcdata = json.load(jcfd)
            jcdata = jcdata['jobCounters']['counterGroup']
            for d in jcdata:
                cntr_grp = d['counterGroupName'].replace('org.apache.hadoop.mapreduce.', '')
                cntrs = d['counter']
                for c in cntrs:
                    if c['totalCounterValue']:
                        print(cntr_grp, c['name'], c['totalCounterValue'], c['mapCounterValue'], c['reduceCounterValue'])
        if i == 5:
            break;
        i = i + 1
    except:
        print(Fore.RED, i, p, 'is invalid', Style.RESET_ALL)
        i = i - 1
        exit()
