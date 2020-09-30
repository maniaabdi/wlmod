import matplotlib
import matplotlib.pyplot as plt
import sys as sys
import numpy as np
from scipy.stats.stats import pearsonr
import pdb

InputFile = open("/local0/container_meta.csv", "r")
path_to_output1 = "/local0/container_app_request.csv"
path_to_output2 = "/local0/container_app_cdf.pdf"

appList = [
    {
        "App" : "app_5052",
        "CID" : ["c_1"],
        "Count" : 1
    }
]

i = 0
for line in InputFile:
    lineSplit = line.split(',')
    App = lineSplit[3]
    #print(App)
    CID = lineSplit[0]
    if not any(item['App'] == App for item in appList):
        tempDict = {
            "App" : App,
            "CID" : [CID],
            "Count" : 1
        }
        appList.append(tempDict)
    else: #App == item["App"]
        for item in appList:
            if ((item['App'] == App) and ( CID not in item["CID"])):
                item["CID"].append(CID) 
                item["Count"] += 1
with open(path_to_output1, 'w') as f:
    for item in appList:
        f.write("%s\n" % item)

lo = 10
hi = 0
for x in (item['Count'] for item in appList):
    lo,hi = min(x,lo),max(x,hi)

#pdb.set_trace()
print("Min is %d" %lo)
print("Max is %d" %hi)

dx = 20
X = np.arange(lo, hi, dx)
Y = np.zeros([len(X)], dtype= int)
for count in (item['Count'] for item in appList):
    for i,x in zip(range(0, len(X)), X):
        if count < x:
            Y[i] +=1

print(Y)
for j,y in zip(range(len(Y)),Y):
    Y[j] = (100 * Y[j])/Y[-1]
    

plt.figure(1, figsize=(6.5, 4.75))
plt.plot(X, Y, 'r--')
plt.ylabel('# of Application')
plt.xlabel('# of Container')
plt.savefig(path_to_output2, format='pdf', dpi=200)

InputFile.close()


