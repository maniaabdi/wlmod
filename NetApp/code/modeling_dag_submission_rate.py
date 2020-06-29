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

# DAG submission rate per day, every 10 minutes, aligend 7AM

from datetime import datetime

weekday = {
    0: 'Mon',
    1: 'Tue',
    2: 'Wed',
    3: 'Thu',
    4: 'Friday',
    5: 'Sat',
    6: 'Sun'
}
def format_xticks10min(x, pos=None):
    hour = (int(x)*600)//3600
    daytime = ''
    if 0 <= hour and hour < 7:
        daytime = 'AM'
        hour += 7;
    elif hour == 7:
        daytime = 'PM'
        hour += 7;
    elif 7 < hour and hour < 19:
        daytime = 'PM'
        hour -= 5;
    elif hour == 19:
        daytime = 'AM'
        hour -= 5;
    elif 19 < hour and hour <= 24:
        daytime = 'AM'
        hour -= 17;
    return str(hour) + daytime

dagsubmission_data = pd.DataFrame()
jobsubmission_data = pd.DataFrame()

for index, f in enumerate(stat_csv):
    # 6Am of each day
    month, day, year = f.split('/')[-1].split('.csv')[0].split('-')
    trace_starttime = datetime.combine(date(int(year), int(month), int(day)), time(7, 0))

    df = pd.read_csv(f)
    df = df[df['submitTime']/1000 > datetime.timestamp(trace_starttime)]

    df['submit_ts'] = (df['submitTime'] - datetime.timestamp(trace_starttime)*1000)//(10*60*1000);

    submitted_dag = pd.DataFrame()
    submitted_dag['submit_ts'] = df.groupby('workflow.id')['submit_ts'].agg('min');
    submitted_dag.sort_values('submit_ts', inplace=True)

    dagsubmission_data[f.split('/')[-1].split('.csv')[0]] = submitted_dag.groupby('submit_ts')['submit_ts'].agg('count')
    jobsubmission_data[f.split('/')[-1].split('.csv')[0]] = df.groupby('submit_ts')['submit_ts'].agg('count')

    if index == 30:
        break

# 50th Percentile
def q50(x):
            return x.quantile(0.5)

# 90th Percentile
def q95(x):
            return x.quantile(0.95)

# DAG submission rate
stats = dagsubmission_data.T.agg(['mean', 'count', 'std', 'median', 'max', 'min', q50, q95])
dagsubmission_data['mean'] = stats.T['mean']
dagsubmission_data['count'] = stats.T['count']
dagsubmission_data['std'] = stats.T['std']
dagsubmission_data['median'] = stats.T['median']
dagsubmission_data['max'] = stats.T['max']
dagsubmission_data['min'] = stats.T['min']
dagsubmission_data['95th'] = stats.T['q95']
dagsubmission_data['50_th'] = stats.T['q50']
dagsubmission_data['ci95_hi'] = dagsubmission_data['mean'] + 1.96*dagsubmission_data['std']/np.sqrt(dagsubmission_data['count'])
dagsubmission_data['ci95_lo'] = dagsubmission_data['mean'] - 1.96*dagsubmission_data['std']/np.sqrt(dagsubmission_data['count'])

fig, ax = plt.subplots(figsize=(10,4))
dagsubmission_data['mean'].plot(ax=ax, color='#7a0177', label='mean')
ax.fill_between(dagsubmission_data.index, (dagsubmission_data['ci95_lo']).values,
                (dagsubmission_data['ci95_hi']).values, color='#feebe2', label=r'${95}^{th}p$')

plt.xticks(np.arange(0, 146, 18), rotation=45)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_xticks10min))
plt.xlabel("Time of day", fontsize=16)
plt.ylabel("# of DAGs \n submitted per 10 mins", fontsize=16)
ax.legend(loc='upper left')

plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
fig.savefig('/home/maniaa/ashes/drawings/fig_querysubmissionrate_mean95.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_querysubmissionrate_mean95.png', format='png', dpi=200)
plt.show()

# Job submission rate
statsj = jobsubmission_data.T.agg(['mean', 'count', 'std', 'median', 'max', 'min', q50, q95])
jobsubmission_data['mean'] = statsj.T['mean']
jobsubmission_data['count'] = statsj.T['count']
jobsubmission_data['std'] = statsj.T['std']
jobsubmission_data['median'] = statsj.T['median']
jobsubmission_data['max'] = statsj.T['max']
jobsubmission_data['min'] = statsj.T['min']
jobsubmission_data['95th'] = statsj.T['q95']
jobsubmission_data['50_th'] = statsj.T['q50']
jobsubmission_data['ci95_hi'] = jobsubmission_data['mean'] + 1.96*jobsubmission_data['std']/np.sqrt(jobsubmission_data['count'])
jobsubmission_data['ci95_lo'] = jobsubmission_data['mean'] - 1.96*jobsubmission_data['std']/np.sqrt(jobsubmission_data['count'])

fig, ax = plt.subplots(figsize=(10,4))
jobsubmission_data['mean'].plot(ax=ax, color='#006837', label='mean')
ax.fill_between(jobsubmission_data.index, (jobsubmission_data['ci95_lo']).values,
                (jobsubmission_data['ci95_hi']).values, color='#ffffcc', label=r'${95}^{th}p$')

plt.xticks(np.arange(0, 146, 18), rotation=45)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_xticks10min))
plt.xlabel("Time of day", fontsize=16)
plt.ylabel("# of submitted jobs\n per 10 mins", fontsize=16)
ax.legend(loc='upper left')

plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
fig.savefig('/home/maniaa/ashes/drawings/fig_jobsubmissionrate_mean95.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_jobsubmissionrate_mean95.png', format='png', dpi=200)
plt.show()
