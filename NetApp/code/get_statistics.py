#!/usr/bin/python

import glob
import os 
import sys
import hadoop.conf.parser as parser

path = "/mnt/nvme0n1/asup-trace/08-19-2018/" 
if len(sys.argv) >= 2:
   path = sys.argv[2]
jobs_history_dir =  [(path + name) for name in os.listdir(path) if os.path.isdir(path + name)]

jobs_history_dir.sort()

print("Number of Jobs:" + str(len(jobs_history_dir)));
i = 0;
exe_plans = []
for path in jobs_history_dir:
#   print(path)
   info = parser.get_job(path)
#   print(info.path)

   all_config = info.get_all();
   print(all_config)

#   all_tasks = info.get_all_tasks()
#   print("task" + str(all_tasks))

#   workflow_id = info.get_workflow_id()
#   print("workflow: " + workflow_id)

   '''
   hive_query = info.get_query();
   print("query: " + hive_query)
   '''
 #  hive_exe_plan = info.get_execution_plan();
 #  exe_plans.append(hive_exe_plan)
 #  print(hive_exe_plan)

   '''
   hive_location = info.get_location();
   if hive_location != 'null':
      exe_plans.append(hive_location)
      print("location: " + hive_location)

   hive_inputdir = info.get_input_dir();
   if hive_inputdir != 'null':
      exe_plans.append(hive_inputdir)
      print("inputdir: " + str(hive_inputdir.split(",")))

   print("\n\n")
   '''
   if i == 20:
      print(len(exe_plans))
      print(len(set(exe_plans)))
      exit(0)
   else:
      i = i + 1

#   input_path = info.get_input();
#   input_size = info.get_input_size();
