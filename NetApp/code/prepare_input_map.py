#!/usr/bin/python

import glob
import os 
import sys
import hadoop.conf.parser as parser


import pandas as pd

print('Read csv file')
df = pd.read_csv('8daysstats.csv')
print(df.columns)

df['submitTimeSec'] = df['submitTime']//1000
start_t = df['submitTimeSec'].min()
df['timestamp'] = df['submitTimeSec'] - start_t

df.sort_values(by=['timestamp'], inplace=True)

print('submittime:', df['timestamp'].max(), df['timestamp'].min())
print('difference:',df['timestamp'].diff().max(), df['timestamp'].diff().min())


# data sharing among users (1)
import json
cross_user_reuse = {}
input_job_data = []
for index, row in df.iterrows(): # iter over jobs
    inputs = row['inputdir'].replace('[', '').replace(']', '').split(',')
    in_tmp = set()
    for x in inputs:
        in_tmp.add(x)
        if x not in cross_user_reuse:
            cross_user_reuse[x] = set()
        cross_user_reuse[x].add(row['user.name'])
    for x in in_tmp:
        input_job_data.append({'user': row['user.name'], 'fpath': x, 'job': row['jobid'], 
                               'timestamp': row['timestamp'], 'runtime': row['runTime']//1000})
    print(index)
input_df = pd.DataFrame(data=input_job_data)
input_df['finishtime'] = input_df['timestamp'] + input_df['runtime']
input_df.to_csv('input_access.csv', index=False)

'''
private_dataset = 0
for x in cross_user_reuse:
    if (len(cross_user_reuse[x])) > 1:
        shared_dataset += 1
    else:
        private_dataset += 1
print(shared_dataset/len(cross_user_reuse), private_dataset/len(cross_user_reuse))
'''




