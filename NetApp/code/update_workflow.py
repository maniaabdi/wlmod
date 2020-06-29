#!/usr/bin/python

import pandas as pd
import ast

df = pd.read_csv('8daysstats.csv')
df = df[df['state']=='SUCCEEDED']

df['workflow.id'] = ""
df['workflow.dag'] = ""
df['workflow.node'] = ""

# loop over jobs,
# for each job list the workflow
for index, row in df.iterrows():
    print(index)
    wrkflw = ast.literal_eval(row['workflow'])
    # per job workflow
    adjacency_list = ''
    for wfed in wrkflw:
        for e in wfed:
            if e == 'mapreduce.workflow.name':
                continue
            if e == 'mapreduce.workflow.node.name':
                df.at[index,'workflow.node'] = wfed[e]
                cur_node = wfed[e]
            if e.startswith('mapreduce.workflow.adjacency'):
                src = e.replace('mapreduce.workflow.adjacency.', '')
                dst = wfed[e]
                adjacency_list += (',' + src + '>' + dst)
            if e == 'mapreduce.workflow.id':
                 df.at[index,'workflow.id'] = wfed[e]
    df.at[index, 'workflow.dag'] = adjacency_list[1:] if len(adjacency_list) > 0 else cur_node;

df = df.drop(columns='workflow')

print(df.columns)
df.to_csv('8daysstats.csv', index=False)
