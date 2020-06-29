#!/usr/bin/python3

import pandas as pd
import json
import os
import sys
import glob
import graph_tool.all as gt
from datetime import datetime, date, time
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
import numpy as np
import math

statistics_dir = '/home/maniaa/ashes/code/statistics/'
stat_csv = [(statistics_dir + f) for f in listdir(statistics_dir) if (f.endswith(".csv") and isfile(join(statistics_dir, f)))]
stat_csv.sort()
stat_csv = stat_csv[1:]


for index, f in enumerate(stat_csv):
    # 6Am of each day
    print(f)
    month, day, year = f.split('/')[-1].split('.csv')[0].split('-')
    trace_starttime = datetime.combine(date(int(year), int(month), int(day)), time(7, 0))

    df = pd.read_csv(f)
    df = df[df['submitTime']/1000 > datetime.timestamp(trace_starttime)]

    df['submit_ts'] = df['submitTime']//1000 - datetime.timestamp(trace_starttime);
    df.sort_values('submit_ts', inplace=True)
    df.reset_index(inplace=True)

    break

def d(data):
    print(data['user.name'])
    print(data['inputdir'])
    print(data['outputdir'])
    print(data['scratchdir'])
    print(data['local.scratchdir'])
    print('\n')


df[['user.name', 'inputdir', 'outputdir', 'scratchdir', 'local.scratchdir']].apply(d, axis=1)
