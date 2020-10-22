import matplotlib
import matplotlib.pyplot as plt
import sys as sys
import numpy as np
from scipy.stats.stats import pearsonr
import pdb

InputFile = open("/local0/container_meta.csv", "r")
path_to_output1 = "/local0/container_app_CPME_request.csv"
path_to_output2 = "/local0/container_app_CPME_number.csv"

appList = [
    {
        "App" : "app_5052",
        "CPME" : [{400, 1.56}],
        "Count" : 1
    }
]

i = 0
for line in InputFile:
    lineSplit = line.split(',')
    App = lineSplit[3]
    CPU = lineSplit[5]
    MEM = lineSplit[7]
    if not any(item['App'] == App for item in appList):
        tempDict = {
            "App" : App,
            "CPME" : [{CPU, MEM}],
            "Count" : 1
        }
        appList.append(tempDict)
    else: #App == item["App"]
        for item in appList:
            if ((item['App'] == App) and ( {CPU, MEM} not in item["CPME"])) :
                item["CPME"].append({CPU, MEM}) 
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
    

