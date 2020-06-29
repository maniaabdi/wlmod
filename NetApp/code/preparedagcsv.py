#!/usr/bin/python3

# build graph pool with the following structure 
# graph_pool = {'graphs': {gid: { 
#                          'nodes': {nodeid: 'label'},
#                          'edges': [{'source': -1, 'target': -1}]}}}
import pandas as pd
import json
import os
import sys
import glob

base_dir = '/home/maniaa/ashes/code/statistics/' 
os.chdir(base_dir)
files = [base_dir + file for file in glob.glob("*.csv")]

graphs = []

def get_n_edges_ngraphs(grp):
    lbl_vid = {}
    wrk_edges = [e.split('>') for e in grp['workflow.dag'].values[0].split(',')]
    graph = {'nodes': {}, 'edges': [], 'n_stages': 0}
    for e in wrk_edges:
        srclbl = int(e[0].split('-')[1])
        if srclbl not in lbl_vid:
            vid = len(lbl_vid)
            lbl_vid[srclbl] = vid
            graph['nodes'][vid] = srclbl
        if len(e) > 1:
            trgtlbl = int(e[1].split('-')[1])
            if trgtlbl not in lbl_vid:
                vid = len(lbl_vid);
                lbl_vid[trgtlbl] = vid
                graph['nodes'][vid] = trgtlbl
            graph['edges'].append({'source': lbl_vid[srclbl], 'target': lbl_vid[trgtlbl]})
        graph['n_stages'] = len(graph['nodes'])
    return graph['n_stages'], len(graph['edges'])

def build_graph_csv(fpath):
    gp = []
    df = pd.read_csv(fpath)
    print(fpath, ' --> processing.')
    wrf_grps = df.groupby('workflow.id')
    for index, grp in wrf_grps:
        n_stage, n_edge = get_n_edges_ngraphs(grp);
        gp.append({'submitTime': grp['submitTime'].min(), 'dag': grp['workflow.dag'].values[0], 'workflow.id': index, 'n_stages': n_stage, 'n_edge': n_edge})
    
    gp_df = pd.DataFrame(data=gp)
    gp_df.to_csv('/home/maniaa/ashes/code/dags.csv', mode='a', header=False);
    return gp

for index, file in enumerate(files):
    build_graph_csv(file)

