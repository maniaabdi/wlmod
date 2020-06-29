#!/usr/bin/python

import json

with open('/mnt/sda/asup-trace/08-04-2018/job_1531656020138_256648/conf.json', 'r') as fd:
    data = json.load(fd)
   # print(json.dumps(data, indent=4, sort_keys=True))
    for conf in data['conf']['property']:
        print(conf['name'], conf['value'])
    #    if conf['name'] == 'mapreduce.workflow.name':
    #        print(conf['name'], conf['value'])
