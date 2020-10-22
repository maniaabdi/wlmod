#!/usr/bin/env python
# coding: utf-8

# In[105]:


import matplotlib
import matplotlib.pyplot as plt
import sys as sys
import numpy as np
from scipy.stats.stats import pearsonr
import pdb

InputFile = open("/local0/batch_instance.csv", "r")
path_to_output1 = "/local0/batch_job_task_instance_machine.csv"

taskItem = [{
        "taskName" : "M1",
        "instance" : 1,
        "instancelist" : [],
        "machine" : 1,
        "machinelist" : [],
        "same_instance_machine" : 0
}]

jobList = [{
        "jobName" : "j_1527",
        "tasklist" : taskItem
}]

i = 0
#print(taskList)

for line in InputFile:
    #print(i)
    #print(line)
    if i > 100000:
        break
    lineSplit = line.split(',')
    Newjob = str(lineSplit[2])
    Newtask =str(lineSplit[1])
    Newinstance = str(lineSplit[0])
    Newmachine = str(lineSplit[7])

    
    jobNameList = []
    for k in range(len(jobList)):
        jobNameList.append(jobList[k]['jobName'])  
    if Newjob not in jobNameList: #new job
        tempTask = [{   
                "taskName" : Newtask,
                "instance" : 1,
                "instancelist" : [Newinstance],
                "machine" : 1,
                "machinelist" : [Newmachine],
                "same_instance_machine" : 0
        }]
        tempDict = {
                "jobName" : Newjob,
                "tasklist" : tempTask,
        }
        jobList.append(tempDict)

    else: #job exists
            for job in jobList:
                if (job['jobName'] == Newjob ): #found it
                        taskNameList = []
                        for j in range(len(job['tasklist'])):
                            taskNameList.append(job['tasklist'][j]['taskName'])
                        if  Newtask not in taskNameList: #new task
                                tempTask = {   
                                        "taskName" : Newtask,
                                        "instance" : 1,
                                        "instancelist" : [Newinstance],
                                        "machine" : 1,
                                        "machinelist" : [Newmachine],
                                        "same_instance_machine" : 0
                                }

                                job['tasklist'].append(tempTask)
                        else: #task exists, add the new instance and machine to the list
                            for j in range(len(job['tasklist'])):
                                if  job['tasklist'][j]['taskName'] == Newtask:
                                    job['tasklist'][j]['instance'] +=1
                                    job['tasklist'][j]['machine'] +=1
                                    if Newinstance in job['tasklist'][j]['instancelist']:
                                        job['tasklist'][j]['same_instance_machine'] +=1
                                    else:
                                        job['tasklist'][j]['instancelist'].append(Newinstance)
                                        job['tasklist'][j]['machinelist'].append(Newmachine)
    i +=1
    #print("job list is:")
    #print(jobList)
                
with open(path_to_output1, 'w') as f:
    for item in jobList:
        f.write("%s\n" % item)


# In[ ]:




