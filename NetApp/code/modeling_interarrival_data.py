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

interarrival_data = pd.DataFrame()

def get_interarrival_values(group):
    return group['interarrival'].values

def get_time_zone(time):
    '''map the access time to three different 
    zones, 4AM-10AM, 10AM-9PM, 9PM-4AM'''
    if  (time > 0) and (time <= 18): # 4AM-10AM
        return 0;
    if (time > 18) and (time <= 84): # 10AM-9PM
        return 1;
    if (time > 84) and (time <= 126): # 9PM-4AM'
        return 2;
    return 0;
    
def aggregate_time_zones(group):
    interarrival_data = []
    for idx, row in group.iteritems():
        interarrival_data.extend(row)
    return interarrival_data

for index, f in enumerate(stat_csv):
    # 7Am of each day
    month, day, year = f.split('/')[-1].split('.csv')[0].split('-')
    trace_starttime = datetime.combine(date(int(year), int(month), int(day)), time(7, 0))
    
    df = pd.read_csv(f)
    df = df[df['submitTime']/1000 > datetime.timestamp(trace_starttime)]
    
    df['submit_ts'] = df['submitTime']//1000 - datetime.timestamp(trace_starttime);
    
    submitted_dag = pd.DataFrame()
    submitted_dag['submit_ts'] = df.groupby('workflow.id')['submit_ts'].agg('min');
    submitted_dag['submit_10min'] = submitted_dag['submit_ts']//(10*60);
    submitted_dag.sort_values('submit_ts', inplace=True)
    submitted_dag['interarrival'] = submitted_dag['submit_ts'].diff().fillna(0)    
    
    col_name = f.split('/')[-1].split('.csv')[0]
    interarrival_data[col_name] = submitted_dag.groupby('submit_10min').apply(get_interarrival_values)
    
    for row in interarrival_data.loc[interarrival_data[col_name].isnull(), col_name].index:
        interarrival_data.at[row, col_name] = np.array([])
    
    
    if 'total' in interarrival_data.columns:
        interarrival_data['total'] = interarrival_data[f.split('/')[-1].split('.csv')[0]].apply(lambda x: x.tolist()) + interarrival_data['total']
    else:
        interarrival_data['total'] = interarrival_data[f.split('/')[-1].split('.csv')[0]].apply(lambda x: x.tolist())        
    if index == 30:
        break
        
time_zones = interarrival_data.groupby(get_time_zone)['total'].apply(aggregate_time_zones)
print(time_zones)


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

def format_yticksprobablity(x, pos=None):
    return str(round(100*x, 1)) + '%'

tzn = {0 : '4AM-10AM',
      1: '10AM-9PM',
      2: '9PM-4AM'}


fig, ax = plt.subplots(figsize=(10,4))
ret = sns.distplot(time_zones[0], fit=stats.lognorm, ax = ax,
             kde_kws={"color": '#252525', "lw": 1, "label": "KDE fit", 'linestyle':'--'}, # Kernel density estimation
             hist_kws={"color": '#999999', "label": "Density"},
             fit_kws={"color": '#b2182b', "lw": 1.5, "label": "Lognormal fit"}); # fitted to exponential
plt.xlabel("Inter arrival time (sec): " + tzn[0], fontsize=16)
plt.ylabel("Density (%)", fontsize=16)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_yticksprobablity))
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
plt.legend()
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivallognorm.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivallognorm.png', format='png', dpi=200)
plt.show()

fig, ax = plt.subplots(figsize=(10,4))
sns.distplot(time_zones[0], fit=stats.expon, ax =ax,
             kde_kws={"color": '#252525', "lw": 1, "label": "KDE fit", 'linestyle':'--'}, # Kernel density estimation
             hist_kws={"color": '#999999', "label": "Density"},
             fit_kws={"color": '#542788', "lw": 1.5, "label": "Exponential fit"});
plt.xlabel("Inter arrival time (sec): " + tzn[0], fontsize=16)
plt.ylabel("Density (%)", fontsize=16)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_yticksprobablity))
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
plt.legend()
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivalexpon.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivalexpon.png', format='png', dpi=200)
plt.show()


fig, ax = plt.subplots(figsize=(10,4))
sns.distplot(time_zones[1], fit=stats.invweibull,
            kde_kws={"color": '#252525', "lw": 1, "label": "KDE fit", 'linestyle':'--'}, # Kernel density estimation
             hist_kws={"color": '#999999', "label": "Density"},
             fit_kws={"color": '#b2182b', "lw": 1.5, "label": "Weibull fit"}); # fitted to exponential );
#data.hist(bins=1000, density=True)
#df['poisson'].diff().dropna().hist(ax=ax, bins=110, density=True)
plt.ylabel("Density (%)", fontsize=16)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_yticksprobablity))
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
plt.legend()
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivalexpon.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrivalexpon.png', format='png', dpi=200)
plt.show()


fig, ax = plt.subplots(figsize=(10,4))
sns.distplot(time_zones[1], bins=100, fit=stats.expon,
            kde_kws={"color": '#252525', "lw": 1, "label": "KDE fit", 'linestyle':'--'}, # Kernel density estimation
             hist_kws={"color": '#999999', "label": "Density"},
             fit_kws={"color": '#b2182b', "lw": 1.5, "label": "Exponetial fit"}); # fitted to exponential );
#data.hist(bins=1000, density=True)
#df['poisson'].diff().dropna().hist(ax=ax, bins=110, density=True)
plt.xlabel("Inter arrival time (sec): " + tzn[1], fontsize=16)
plt.ylabel("Density (%)", fontsize=16)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_yticksprobablity))
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
plt.legend()
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrival10am9pmexpon.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histograminterarrival10am9pmexpon.png', format='png', dpi=200)
plt.show()


fig, ax = plt.subplots(figsize=(10,4))
sns.distplot(time_zones[2], fit=stats.skewnorm);
#data.hist(bins=1000, density=True)
#df['poisson'].diff().dropna().hist(ax=ax, bins=110, density=True)
plt.xlabel("Inter arrival time (sec): " + tzn[2], fontsize=16)
plt.ylabel("Density", fontsize=16)
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
fig.savefig('/home/maniaa/ashes/drawings/fig_histogramsubmissionratewpoissin.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histogramsubmissionratewpoissin.png', format='png', dpi=200)
plt.show()

fig, ax = plt.subplots(figsize=(10,4))
sns.distplot(time_zones[2], fit=stats.invweibull);
#data.hist(bins=1000, density=True)
#df['poisson'].diff().dropna().hist(ax=ax, bins=110, density=True)
plt.xlabel("Inter arrival time (sec): " + tzn[2], fontsize=16)
plt.ylabel("Density", fontsize=16)
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
fig.savefig('/home/maniaa/ashes/drawings/fig_histogramsubmissionratewpoissin.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_histogramsubmissionratewpoissin.png', format='png', dpi=200)
plt.show()

