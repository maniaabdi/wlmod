import pandas as pd
import numpy as np
import sys as sys
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

#path_to_csv= "/local0/machine_usage_new.csv"
#path_to_csv_machine= "/local0/machineId.csv"
#path_to_csv_cpu= "/local0/cpuUsage.csv"
#path_to_csv_mem= "/local0/memUsage.csv"
#path_to_csv_time= "/local0/time.csv"

path_to_csv_machine= sys.argv[1] 
path_to_csv_time= sys.argv[2]
path_to_csv_1= sys.argv[3]
path_to_output= sys.argv[4]

#cpuUsage = (4035, 2305)
cpuUsage = np.zeros([4035, 2305], dtype=float)
time = np.zeros([2305], dtype= int)
machineId = np.zeros([4035], dtype= int)
countM = -1
countT = 0
PreMachineId = 0
PreTime = 300

with open(path_to_csv_machine) as machineFile, open(path_to_csv_time) as timeFile, open(path_to_csv_1) as cpuFile:
    for lineMachine, lineTime, lineCpu in zip(machineFile, timeFile, cpuFile):

        if PreMachineId != lineMachine:
            PreMachineId = lineMachine
            countM = int(PreMachineId[2:])
            machineId[countM] = int(PreMachineId[2:])

        while PreTime != int(lineTime):
            #import pdb; pdb.set_trace()
            PreTime +=300
            countT += 1
            if PreTime > 691200:
                PreTime = 300
                countT = 0
        time[countT] = int(lineTime)
        cpuUsage[countM, countT] = float(lineCpu)

    
print("done while")

num_ticks = 10
yticks = np.linspace(0, len(machineId) - 1, num_ticks, dtype=np.int)
xticks = np.linspace(0, len(time)/12 - 1, num_ticks, dtype=np.int)



axcpu = plt.subplots(figsize=(6.5, 4.75))
axcpu = sns.heatmap(cpuUsage,cmap='jet')
#axcpu.set_yticks(yticks)
#axcpu.set_xticks(xticks)
axcpu.set(xlabel='Time(hours)', ylabel='Machine ID')
plt.subplots_adjust(left=0.12, bottom=0.20, right=0.98, top=0.98, wspace=None, hspace=None)

plt.savefig(path_to_output, format='pdf', dpi=200)

#plt.show()

