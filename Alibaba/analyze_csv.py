
InputFile = open("/local0/machine_usage_new.csv", "r")
netInFile = open("/local0/netInUsage.csv", "w")
netOutFile = open("/local0/netOutUsage.csv", "w")
diskFile = open("/local0/diskUsage.csv", "w")

time = 300
#timeFile.write(str(time)+ '\n')
count = 0
netInTotal = 0
netOutTotal = 0
diskTotal = 0
machineCount = 0
machineId = ""
for line in InputFile:
    lineSplit = line.split(',')
    if lineSplit[0] != '':
        machineIdnow = lineSplit[0]
    else:
        continue
    if lineSplit[1] != '':
        timeStamp = int(lineSplit[1])
    else:
        continue
    if lineSplit[6] != '':
        netInUsage = float(lineSplit[6])
    else:
        netInUsage = 0
    if lineSplit[7] != '':
        netOutUsage = float(lineSplit[7])
    else:
        netOutUsage = 0
    if lineSplit[8] != '':
        diskUsage = float(lineSplit[8])
    else:
        diskUsage = 0

    if machineIdnow != machineId:
        if count > 0:
            netInAve = netInTotal/count
            netOutAve = netOutTotal/count
            diskAve = diskTotal/count
            netInFile.write(str(netInAve)+ '\n')
            netOutFile.write(str(netOutAve)+ '\n')
            diskFile.write(str(diskAve)+ '\n')

        netInTotal = 0
        netOutTotal = 0
        diskTotal = 0
        count = 0
        time = 300
        #timeFile.write(str(time)+ '\n')
        machineId = machineIdnow
        #machineFile.write(str(machineId)+ '\n')

    if timeStamp < time:
        count += 1
        netInTotal += netInUsage
        netOutTotal += netOutUsage
        diskTotal += diskUsage
    else:
        if count > 0:
            netInAve = netInTotal/count
            netOutAve = netOutTotal/count
            diskAve = diskTotal/count
            netInFile.write(str(netInAve)+ '\n')
            netOutFile.write(str(netOutAve)+ '\n')
            diskFile.write(str(diskAve)+ '\n')
        netInTotal = 0
        netOutTotal = 0
        diskTotal = 0
        count = 0
        while timeStamp >= time:
            time += 300
            #timeFile.write(str(time)+ '\n')
        count += 1
        netInTotal += netInUsage
        netOutTotal += netOutUsage
        diskTotal += diskUsage

netInFile.close()
netOutFile.close()
diskFile.close()
InputFile.close()


