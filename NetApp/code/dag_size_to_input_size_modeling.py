#!/usr/bin/python3
#import matplotlib
#from matplotlib import rc

#import matplotlib
#from matplotlib import rc
#rc('text',usetex=True)
#rc('text.latex', preamble=r'\usepackage{color}')
#import matplotlib as mpl
#mpl.rcParams.update(mpl.rcParamsDefault)

import matplotlib as matplotlib
matplotlib.use('pgf')
matplotlib.rc('pgf', texsystem='pdflatex', preamble=r'\usepackage{color}')  # from running latex -v
#preamble = matplotlib.rcParams.setdefault('pgf.preamble', [])
#preamble=r'\usepackage{color}'


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import pandas as pd
import numpy as np
import math

import json
from os import listdir
from os.path import isfile, join
import sys
import glob
import graph_tool.all as gt
from datetime import datetime, date, time


statistics_dir = '/home/maniaa/ashes/code/statistics/'
stat_csv = [(statistics_dir + f) for f in listdir(statistics_dir) if (f.endswith(".csv") and isfile(join(statistics_dir, f)))]
stat_csv.sort()
stat_csv = stat_csv[1:]




# for multiple
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
    n_2t5j = len(group[(group['dag_nv'] > 1) & (group['dag_nv'] < 5)])
    n_10j = len(group[group['dag_nv'] >= 5])
    dag_submission_stats.append({'submit_10min': int(group['submit_10min'].max()),
                                 '1': n_1j, '2<5': n_2t5j, '>5': n_10j})

    io_sz_1j = group[group['dag_nv'] == 1]['HDFS_INPUT_SIZE'].sum()
    io_sz_2t5j = group[(group['dag_nv'] > 1) & (group['dag_nv'] < 5)]['HDFS_INPUT_SIZE'].sum()
    io_sz_10j = group[group['dag_nv'] >= 5]['HDFS_INPUT_SIZE'].sum()
    dag_submission_iosize.append({'submit_10min': int(group['submit_10min'].max()),
                                 '1': io_sz_1j, '2<5': io_sz_2t5j, '>5': io_sz_10j})



def input_size_group(size):
    if size < 1: return 0;

    log = math.log2(int(size));
    if log < 20: # < 1M
        return 0;

    if log >= 20 and log < 27: # 1M - 128M
        return 1;

    if log >= 27 and log < 31: # 128M - 2G
        return 2;

    if log >= 31 and log < 35: # 2G - 32G
        return 3;

    if log >= 35 and log < 38: # 32G - 256G
        return 4;

    if log >= 38 and log < 40: # 256G-1T
        return 5;

    return 6; # log >= 40 (1T)


def tame_io_sizes(group, job_sz_io_sz):
    job_sz_io_sz.append({'input_size': group['HDFS_INPUT_SIZE'].sum(),
                        'input_group': input_size_group(group['HDFS_INPUT_SIZE'].sum()),
                        'dag_size': group['dag_nv'].max(),
                        'submit_ts': group['submit_10min'].min()})


def format_xticks_iosize(x, pos=None):
    index_to_size = {0: r'$<$ 1MB', 1: '1MB - 128MB',
                    2: '128MB - 2GB', 3: '2MB - 32GB',
                    4: '32GB - 256GB', 5: '256GB - 1TB',
                    6: r'$>$ 1TB',}
    return index_to_size[x]

def format_annotation(size):
    log = int(math.log2(size))
    if log < 10:
        return str(size) + ' B'
    if log >= 10 and log < 20:
        return str(2**(log - 10)) + ' KB'
    if log >= 20 and log < 30:
        return str(2**(log - 20)) + ' MB'
    if log >= 30 and log < 40:
        return str(2**(log - 30)) + ' GB'
    if log >= 40:
        return str(2**(log - 40)) + ' TB'
    return str(size)

def rotation(height):
    if height > 50:
        return 0
    return 90;


dag_submission_stats = []
dag_submission_iosize = []
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

    job_sz_io_sz = [];
    df.groupby('workflow.id').apply(tame_io_sizes, job_sz_io_sz)

    js_is_df = pd.DataFrame(job_sz_io_sz)
    jsg_df = pd.DataFrame();
    jsg_df['Single job'] = js_is_df[js_is_df['dag_size'] == 1].groupby('input_group')['dag_size'].agg('count')
    jsg_df['2 < DAGsize < 5'] = js_is_df[(js_is_df['dag_size'] > 1) & (js_is_df['dag_size'] < 5)].groupby('input_group')['dag_size'].agg('count')
    jsg_df['DAGsize > 5'] = js_is_df[js_is_df['dag_size'] > 5 ].groupby('input_group')['dag_size'].agg('count')
    jsg_df['1jis'] = js_is_df[js_is_df['dag_size'] == 1].groupby('input_group')['input_size'].agg('sum')
    jsg_df['2<5jis'] = js_is_df[(js_is_df['dag_size'] > 1) & (js_is_df['dag_size'] < 5)].groupby('input_group')['input_size'].agg('sum')
    jsg_df['jis5'] = js_is_df[js_is_df['dag_size'] >= 5 ].groupby('input_group')['input_size'].agg('sum')
    jsg_df.fillna(1, inplace=True)

    print(jsg_df)

    single, j25, j5, s1, s2, s3 = jsg_df.sum()
    jsg_df['Single jobs'] = 100*jsg_df['Single job']/single
    jsg_df['2 < DAG size < 5'] = 100*jsg_df['2 < DAGsize < 5']/j25
    jsg_df['DAG size > 5'] = 100*jsg_df['DAGsize > 5']/j5

    colors = ['#80b1d3', '#ff7f00', '#e41a1c']
    fig, ax = plt.subplots(figsize=(10,4))
    jsg_df[['Single jobs', '2 < DAG size < 5', 'DAG size > 5']].plot.bar(ax=ax, color=colors, width=0.9)
    plt.xticks(rotation=5)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_xticks_iosize))
    ax.set_ylim([0,100])
    #ax.set_yscale('log')
    plt.xlabel("Input size", fontsize=16)
    plt.ylabel(r'\% of DAGs', fontsize=16)
    plt.legend([r'Single jobs', r'2 $<$ DAG size $<$ 5', r'DAG size $>=$ 5'])

    # set individual bar lables using above list
    for idx, i in enumerate(ax.patches):
        ax.text(i.get_x()+.04, i.get_height()+3,
                r'{\textcolor{blue}{' + format_annotation(jsg_df.iloc[idx%len(jsg_df), 3 + (idx//len(jsg_df))]) + r'}}' \
                + ', ' + r'{\textcolor{red}{' + str(jsg_df.iloc[idx%len(jsg_df), (idx//len(jsg_df))]) + r'}}',
                fontsize=11, rotation=rotation(i.get_height()))

    plt.subplots_adjust(left=0.1, bottom=0.2, right=0.96, top=0.97)
    fig.savefig('/home/maniaa/ashes/drawings/fig_dagsizevsinputsize_' +  f.split('/')[-1].split('.csv')[0]  + '.pdf', format='pdf', dpi=200)
    fig.savefig('/home/maniaa/ashes/drawings/fig_dagsizevsinputsize_' + f.split('/')[-1].split('.csv')[0] + '.png', format='png', dpi=200)

    if index == 30:
        break
