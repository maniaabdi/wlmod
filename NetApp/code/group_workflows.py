#!/usr/bin/python

import glob
import os 
import sys
import pandas as pd
from collections import OrderedDict
from datetime import date
import hadoop.conf.parser as parser

path = "/mnt/nvme0n1/asup-trace/09-19-2018/" 
if len(sys.argv) >= 2:
   path = sys.argv[2]
jobs_history_dir =  [(path + name) for name in os.listdir(path) if os.path.isdir(path + name)]

jobs_history_dir.sort()

#print("Number of Jobs:" + str(len(jobs_history_dir)));
i = 0;
# username': u'asupdl', 'framework': u'hive', 'date': u'20180918235959', 'id': u'dd639ee6-d011-4161-8b6e-285110ac921c'
workflow_meta_df = pd.DataFrame(columns=['username', 'framework', 'date', 'workflowid', 'query', 'external_location', 'input_dir'])
for path in jobs_history_dir:
   #print(path)
   info = parser.get_job(path)

   wfm = info.get_workflow_meta()
   if not wfm:
       continue;

   #print(info.path)
   query = info.get_query(); 
   if query:
       wfm.update(query)

   external_location = info.get_external_location();
   if external_location:
       #print("\t" + str(external_location))
       wfm.update(external_location)

   input_dir = info.get_input_dir();
   if input_dir:
       #print("\t" + str(input_dir))
       wfm.update(input_dir)

   if (external_location is None) and (input_dir is None):
       print(info.get_all())
       exit()

   workflow_meta_df = workflow_meta_df.append(wfm, ignore_index=True)

#   if i == 10000:
#      break
#   else:
#      i = i + 1


workflow_meta_grp = workflow_meta_df.groupby(['username']) 
pd.set_option('expand_frame_repr', True)
pd.set_option('max_colwidth', 200)

#print(workflow_meta_grp.size())
#print(workflow_meta_grp.get_group('saia')['workflowid'])
#print(workflow_meta_grp.get_group('saia')['workflowid'].nunique())

for name, group in workflow_meta_grp:
    print(name + "\t" + str(group['workflowid'].count()) + "\t" + str(group['workflowid'].nunique()))

#lvl0 = workflow_meta_df.username.values
#lvl1 = workflow_meta_df.workflowid.values
#lvl2 = workflow_meta_df.date.values
#lvl3 = workflow_meta_df.framework.values


#index = pd.MultiIndex.from_arrays([lvl0, lvl1], names=['username', 'workflowid'])
#df = pd.DataFrame({'date': lvl2},
#        index=index)
#print(df.to_string())

#print(midx)

#workflow_meta_df.set_index(['username', 'workflowid'], inplace=True)
#print(workflow_meta_df.to_string())
#   input_path = info.get_input();
#   input_size = info.get_input_size();
