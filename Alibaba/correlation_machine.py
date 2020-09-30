import matplotlib
import matplotlib.pyplot as plt
import sys as sys
import numpy as np
from scipy.stats.stats import pearsonr

InputFile = open("/local0/machine_usage_new.csv", "r")
path_to_output1 = "/local0/CpuMem_Cor.pdf"
path_to_output2 = "/local0/CpuNetIn_Cor.pdf"
path_to_output3 = "/local0/CpuNetOut_Cor.pdf"
path_to_output4 = "/local0/CpuDisk_Cor.pdf"

path_to_output5 = "/local0/MemNetIn_Cor.pdf"
path_to_output6 = "/local0/MemNetOut_Cor.pdf"
path_to_output7 = "/local0/MemDisk_Cor.pdf"

machineId = 0
count= 0

tempArray = np.zeros([1], dtype = float)
#machineIdArray = np.zeros([4036], dtype=int)
machineIdArray = np.zeros([], dtype=int)
cpuArray = np.zeros([], dtype=float)
memArray = np.zeros([], dtype=float)
netInArray = np.zeros([], dtype=float)
netOutArray = np.zeros([], dtype=float)
diskArray = np.zeros([], dtype=float)


CpuMem = np.zeros([], dtype=float)
CpuNetIn = np.zeros([], dtype=float)
CpuNetOut= np.zeros([], dtype=float)
CpuDisk = np.zeros([], dtype=float)

MemNetIn = np.zeros([], dtype=float)
MemNetOut= np.zeros([], dtype=float)
MemDisk = np.zeros([], dtype=float)


for line in InputFile:
    count += 1
    if count == 27428943:
        break
    lineSplit = line.split(',')
    if lineSplit[0] != '':
        if int(lineSplit[0][2:]) != machineId:

            CpuMem = np.append(CpuMem, np.corrcoef(cpuArray, memArray)[0][1])
            CpuNetIn = np.append(CpuNetIn, np.corrcoef(cpuArray, netInArray)[0][1])
            CpuNetOut = np.append(CpuNetOut, np.corrcoef(cpuArray, netOutArray)[0][1])
            CpuDisk = np.append(CpuDisk, np.corrcoef(cpuArray, diskArray)[0][1])

            MemNetIn = np.append(MemNetIn, np.corrcoef(memArray, netInArray)[0][1])
            MemNetOut = np.append(MemNetOut, np.corrcoef(memArray, netOutArray)[0][1])
            MemDisk = np.append(MemDisk, np.corrcoef(memArray, diskArray)[0][1])

            machineId = int(lineSplit[0][2:])
            cpuArray = tempArray
            memArray = tempArray
            netInArray = tempArray
            netOutArray = tempArray
            diskArray = tempArray

            machineIdArray = np.append(machineIdArray, machineId)

            print(machineId)

        else:
            if lineSplit[2] != '':
                cpuArray = np.append(cpuArray, float(lineSplit[2]))
            else:
                cpuArray = np.append(cpuArray, 0)

            if lineSplit[3] != '':
                memArray = np.append(memArray, float(lineSplit[3]))
            else:
                memArray = np.append(memArray, 0)
            if lineSplit[6] != '':
                netInArray = np.append(netInArray, float(lineSplit[6]))
            else:
                netInArray = np.append(netInArray, 0)
            if lineSplit[7] != '':
                netOutArray = np.append(netOutArray, float(lineSplit[7]))
            else:
                netOutArray = np.append(netOutArray, 0)
            if lineSplit[8] != '':
                diskArray = np.append(diskArray, float(lineSplit[8]))
            else:
                diskArray = np.append(diskArray, 0)
    else:
        continue


print(CpuMem)
print(CpuNetIn)
print(CpuNetOut)
print(CpuDisk)


plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, CpuMem, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output1, format='pdf', dpi=200)


plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, CpuNetIn, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output2, format='pdf', dpi=200)

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, CpuNetOut, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output3, format='pdf', dpi=200)

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, CpuDisk, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output4, format='pdf', dpi=200)

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, MemNetIn, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output5, format='pdf', dpi=200)

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, MemNetOut, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output6, format='pdf', dpi=200)

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(machineIdArray, MemDisk, 'ro')
plt.ylabel('Correlation')
plt.xlabel('Machine ID')
plt.savefig(path_to_output7, format='pdf', dpi=200)



InputFile.close()


