import matplotlib
import matplotlib.pyplot as plt
import sys as sys
import numpy as np
from scipy.stats.stats import pearsonr
import pdb

InputFile = open("/local0/container_meta.csv", "r")
path_to_output1 = "/local0/machine_app_request.csv"
path_to_output2 = "/local0/machine_app_number.csv"

appList = [
    {
        "App" : "app_5052",
        "MCH" : ['m_2556'],
        "Count" : 1
    }
]

i = 0
for line in InputFile:
    lineSplit = line.split(',')
    App = lineSplit[3]
    MCH = lineSplit[1]
    if not any(item['App'] == App for item in appList):
        tempDict = {
            "App" : App,
            "MCH" : [MCH],
            "Count" : 1
        }
        appList.append(tempDict)
    else: #App == item["App"]
        for item in appList:
            if ((item['App'] == App) and ( MCH not in item["MCH"])) :
                item["MCH"].append(MCH) 
                item["Count"] += 1

with open(path_to_output1, 'w') as f:
    for item in appList:
        f.write("%s\n" % item)


#print(item["Count"] for item in appList if (item["Count"] > 1))
with open(path_to_output2, 'w') as fa:
    for item in appList:
        if (item["Count"] > 1):
            print(item["Count"])
            fa.write("%s: %d\n" % (item["App"], item["Count"]))
    

