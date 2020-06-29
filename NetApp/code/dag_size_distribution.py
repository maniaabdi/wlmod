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


# job size which is number of mappers + number of reducers
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

def build_graph(data):
    gstr = data['workflow.dag']
    wrk_edges = [e.split('>') for e in gstr.split(',')]
    lbl_vid = {}
    graph = {'nodes': {}, 'edges': []}
    g = gt.Graph(directed=True)
    v_lbl = g.new_vertex_property("int")
    for e in wrk_edges:
        srclbl = int(e[0].split('-')[1])
        if srclbl not in lbl_vid:
            vsrc = g.add_vertex()
            v_lbl[vsrc] = srclbl
            lbl_vid[srclbl] = int(vsrc)

        if len(e) > 1:
            trgtlbl = int(e[1].split('-')[1])
            if trgtlbl not in lbl_vid:
                vtgt = g.add_vertex()
                v_lbl[vtgt] = trgtlbl
                lbl_vid[trgtlbl] = int(vtgt)
            g.add_edge(lbl_vid[srclbl], lbl_vid[trgtlbl])

    g.vertex_properties['label'] = v_lbl
    data['dag_nv'] = g.num_vertices()
    data['dag_ne'] = g.num_edges()
    return data

def dag_size_per_timebin(group):
    global dag_submission_stats
    n_1j = len(group[group['dag_nv'] == 1])
    n_2t5j = len(group[(group['dag_nv'] > 2) & (group['dag_nv'] <= 5)])
    n_10j = len(group[group['dag_nv'] > 10])
    dag_submission_stats.append({'submit_10min': int(group['submit_10min'].max()),
                                 '1': n_1j, '2<5': n_2t5j, '>5': n_10j})

dag_submission_stats = []
for index, f in enumerate(stat_csv):
    print(f) # 6Am of each day
    month, day, year = f.split('/')[-1].split('.csv')[0].split('-')
    trace_starttime = datetime.combine(date(int(year), int(month), int(day)), time(7, 0))

    df = pd.read_csv(f)
    df = df[df['submitTime']/1000 > datetime.timestamp(trace_starttime)]
    df = df[df['state'] == 'SUCCEEDED']
    df['submit_ts'] = df['submitTime']//1000 - datetime.timestamp(trace_starttime);
    df['submit_10min'] = df['submit_ts']//(10*60);
    df.sort_values('submit_ts', inplace=True)
    df.reset_index(inplace=True)
    df = df.apply(build_graph, axis=1)
    df.groupby('submit_10min').apply(dag_size_per_timebin)

    if index == 30:
        break

dt = pd.DataFrame(dag_submission_stats).groupby('submit_10min')[['1', '2<5', '>5']].agg('sum')
dt['total'] = dt['1'] + dt['2<5'] + dt['>5']
dt['Single jobs'] = 100*dt['1']/dt['total']
dt['2 < DAG size < 5'] = 100*dt['2<5']/dt['total']
dt['DAG size > 5'] = 100*dt['>5']/dt['total']

colors = ['#80b1d3', '#ff7f00', '#e41a1c']
fig, ax = plt.subplots(figsize=(8,4))
dt[['Single jobs', '2 < DAG size < 5', 'DAG size > 5']].plot.area(ax=ax, color=colors, stacked=True);
plt.xticks(np.arange(0, 146, 18), rotation=45)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_xticks10min))
plt.xlabel("Time of day (30 days)", fontsize=16)
plt.ylabel("% of DAGs size \n per 10 mins", fontsize=16)
plt.legend()
plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
fig.savefig('/home/maniaa/ashes/drawings/fig_dagsizeper10min.pdf', format='pdf', dpi=200)
fig.savefig('/home/maniaa/ashes/drawings/fig_dagsizeper10min.png', format='png', dpi=200)
plt.show()

dt.to_csv('/home/maniaa/ashes/dag_size_distribution_per_10min.csv')
