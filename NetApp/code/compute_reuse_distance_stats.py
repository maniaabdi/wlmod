#!/usr/bin/python3

import pandas as pd
import numpy as np

# compute reuse distance time
df = pd.read_csv('input_access.csv')
print(df.columns)

df2 = pd.read_csv('../conf.csv')
print(df2.columns)

ru_dist_stats = []
dfg = df.groupby('fpath')
for index, group in dfg:
    reuse_distance_time = group['timestamp'].diff().dropna().astype('int64').values
    if len(reuse_distance_time) == 0:
        continue
    print(index)
    ru_dist_stats.append({
        'name': index,
        'frequency': len(reuse_distance_time) + 1,
        'max':  reuse_distance_time.max(), 
        'min': reuse_distance_time.min(), 
        'mean': reuse_distance_time.mean(), 
        'avg': reuse_distance_time.std(), 
        'std': reuse_distance_time.std(), 
        '25thq': np.percentile(reuse_distance_time, 25),
        '50th': np.percentile(reuse_distance_time, 50), 
        '75th': np.percentile(reuse_distance_time, 75)})
df = pd.DataFrame(data=ru_dist_stats)
df.to_csv('input_access_stats.csv')
