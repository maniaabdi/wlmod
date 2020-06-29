#!/usr/bin/python3

import parser
import threadpool as tp

import glob
import os 
import sys
import pandas as pd 


def parse_jobs_ofday(path2):
    jobs_history_dir = [(path2 +'/' + name) for name in os.listdir(path2) if os.path.isdir(path2+ '/' + name)]
    day = path2.split('/')[-1]
    print(len(jobs_history_dir))
    data = []
    for index, p in enumerate(jobs_history_dir):
        if index%1000 == 0: 
            print(index)
        stats = parse_job_stats(p)
        if stats:
            data.append(stats)
    df = pd.DataFrame(data)
    df.to_csv('statistics/' + day +'.csv', index=False)

def parse_job_stats(p):
    jtype = parser.get_job_type(p)
    if jtype == 'hive':
        counters = parser.get_job_counters(p)
        stats = parser.get_job_stats(p)
        conf = parser.get_hive_job_configurations(p)
        #io_access = build_io_dependency(conf, io_access, reaccess)
        stats.update(counters)
        stats.update(conf)
        return stats

path = "/mnt/sda/asup-trace/"
days_dirs = [path + s for s in ['08-05-2018']]

#days_dirs = [path + s for s in ['08-12-2018']]
print(days_dirs)
pool = tp.ThreadPool(7)
pool.map(parse_jobs_ofday, days_dirs)
pool.wait_completion()
