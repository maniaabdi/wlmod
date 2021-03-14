#!/usr/bin/python3
import sys

rd_time = {}
rd_size = {}
rd_size_t = {}

fpath = 'workload.gen'
total_size = 0
with open(fpath, 'r') as fd:
    accesses = [ln.split(' ') for ln in fd.read().split('\n')[:-1]]
    for time, obj, size in accesses:
        if obj not in rd_size_t:
            rd_size_t[obj] = []

        for alt in rd_size_t:
            if alt != obj:
                rd_size_t[alt].append(int(size))
            else:
                 rd_size_t[alt].append(0)
        total_size += int(size)

#for alt in rd_size_t:
#    if alt not in rd_size:
#        rd_size[alt] = [-1]


rd_size = {} 
for obj in rd_size_t:
    if obj not in rd_size:
        rd_size[obj] = []

    reuse_distance = 0
    for d in rd_size_t[obj][1:]:
        if not d:
            rd_size[obj].append(reuse_distance)
            reuse_distance = 0
        else:
            reuse_distance += d
    #if reuse_distance and rd_size_t[obj][-1]:
    #    rd_size[obj].append(total_size)

for obj in rd_size:
    print(obj, rd_size[obj])
