#!/usr/bin/python3

import glob
import os 
import sys
import pandas as pd
import numpy as np
import json
import math
from colorama import Fore, Back, Style 


def get_job_counters(p):
    try:
       with open(p+'/job_counters.json', 'r') as jcfd:
            counters = {'HDFS_INPUT_SIZE': 0, 'HDFS_OUTPUT_SIZE': 0, 'MAP_CPU_USAGE_MSEC': 0, 'REDUCE_CPU_USAGE_MSEC': 0, 'MAP_MEM_USAGE_B': 0,
                    'REDUCE_MEM_USAGE_B': 0, 'HIVE_RECORDS_IN': 0, 'HIVE_RECORDS_OUT': 0, 'HIVE_RECORDS_INTERMEDIATE': 0,
                    'SLOTS_MILLIS_MAPS': 0, 'SLOTS_MILLIS_REDUCES': 0, 'TOTAL_LAUNCHED_MAPS': 0, 'TOTAL_LAUNCHED_REDUCES': 0, 'DATA_LOCAL_MAPS': 0,  'RACK_LOCAL_MAPS': 0,
                    'MILLIS_MAPS': 0, 'MILLIS_REDUCES': 0, 'VCORES_MILLIS_MAPS': 0, 'VCORES_MILLIS_REDUCES': 0, 'MB_MILLIS_MAPS': 0, 'MB_MILLIS_REDUCES': 0}
            jcdata = json.load(jcfd)
            if 'counterGroup' not in jcdata['jobCounters']:
                return counters;

            jcdata = jcdata['jobCounters']['counterGroup']
            for d in jcdata:
                cntr_grp = d['counterGroupName'].replace('org.apache.hadoop.mapreduce.', '')
                cntrs = d['counter']
                for c in cntrs:
                    if cntr_grp == 'HIVE':
                        if c['name'] == 'RECORDS_IN':
                            counters['HIVE_RECORDS_IN'] = c['totalCounterValue']
                        if c['name'] == 'RECORDS_OUT_0':
                            counters['HIVE_RECORDS_OUT'] = c['totalCounterValue']
                        if c['name'] == 'RECORDS_OUT_INTERMEDIATE':
                            counters['HIVE_RECORDS_INTERMEDIATE'] = c['totalCounterValue']
                    elif cntr_grp == 'FileSystemCounter':
                        if c['name'] == 'HDFS_BYTES_READ':
                            counters['HDFS_INPUT_SIZE'] = c['totalCounterValue']
                        if c['name'] == 'HDFS_BYTES_WRITTEN':
                            counters['HDFS_OUTPUT_SIZE'] = c['totalCounterValue']
                    elif cntr_grp == 'TaskCounter':
                        if c['name'] == 'PHYSICAL_MEMORY_BYTES':
                            counters['PHMAP_MEM_USAGE_B'] = c['mapCounterValue']
                            counters['PHREDUCE_MEM_USAGE_B'] = c['reduceCounterValue']
                            counters['PHPHYSICAL_MEMORY_B'] = c['totalCounterValue']
                        if c['name'] == 'VIRTUAL_MEMORY_BYTES':
                            counters['MAP_MEM_USAGE_B'] = c['mapCounterValue']
                            counters['REDUCE_MEM_USAGE_B'] = c['reduceCounterValue']
                        if c['name'] == 'CPU_MILLISECONDS':
                            counters['MAP_CPU_USAGE_MSEC'] = c['mapCounterValue']
                            counters['REDUCE_CPU_USAGE_MSEC'] = c['reduceCounterValue']
                    elif cntr_grp == 'JobCounter':
                        if (c['name'] not in counters):
                            if (c['name'] in ['NUM_KILLED_REDUCES', 'NUM_KILLED_MAPS', 'OTHER_LOCAL_MAPS', 'NUM_FAILED_MAPS', 'NUM_FAILED_REDUCES']):
                                continue;
                            raise NameError('Invalid counter: ' + c['name'])
                        counters[c['name']] = c['totalCounterValue']

            return counters
    except IOError as e:
        print(Fore.RED, p, 'is invalid: job_counters', Style.RESET_ALL)


def get_job_stats(path):
    job_id = path.split('/')[-1]
    try:
        with open(path+'/' + job_id  +'.json', 'r') as json_file:
            ResJson= json.load(json_file)
            return {'state': ResJson['job']['state'],
                    'submitTime':  ResJson['job']['submitTime'],
                    'startTime':  ResJson['job']['startTime'],
                    'finishTime': ResJson['job']['finishTime'],
                    'queueTime': ResJson['job']['startTime'] - ResJson['job']['submitTime'],
                    'runTime': ResJson['job']['finishTime'] - ResJson['job']['startTime'],
                    'NumMaps': ResJson['job']['mapsTotal'],
                    "avgMapTime": ResJson['job']['avgMapTime'],
                    "avgReduceTime": ResJson['job']['avgReduceTime'],
                    "avgShuffleTime":ResJson['job']['avgShuffleTime'],
                    "avgMergeTime":  ResJson['job']['avgMergeTime'],
                    'NumReduce': (ResJson['job']['reducesTotal'] if 'reduceTotal' in ResJson['job'] else 0)}
    except IOError as e:
        print('cannot read', path+'/' + self.job  +'.json')

def get_hive_job_configurations(path):
    p = path
    configs = {}
    try:
        job_id = p.split('/')[-1]
        with open(p+'/conf.json', 'r') as fd:
            data = json.load(fd)
            configs['jobid'] = job_id
            for conf in data['conf']['property']:
                if conf['name'] == 'mapreduce.job.maps':
                    configs['job.maps'] = conf['value']
                if conf['name'] == 'name':
                    configs['table.name'] = conf['value']
                if conf['name'] == 'mapreduce.job.user.name':
                    configs['user.name'] = conf['value']
                elif conf['name'] == 'mapreduce.job.name':
                    configs['job'] = conf['value']
                elif conf['name'] == 'hive.query.id':
                    configs['query.id'] = conf['value']
                elif conf['name'] == 'hive.query.string':
                    configs['query'] = conf['value']
                elif conf['name'] == 'hive.exec.scratchdir':
                    configs['scratchdir'] = conf['value']
                elif conf['name'] == 'hive.exec.local.scratchdir':
                    configs['local.scratchdir'] = conf['value']
                elif conf['name'] == 'hive.session.id':
                    configs['sessionid'] = conf['value']
                elif conf['name'] == '_hive.hdfs.session.path':
                    configs['outputdir'] = conf['value']
                elif conf['name'] == 'hive.stats.tmp.loc':
                    configs['tmpouput'] = conf['value']
                elif conf['name'] == 'mapreduce.input.fileinputformat.inputdir':
                    inputs = []
                    configs['n_inputs'] = len(conf['value'].split(','))
                    configs['inputdir'] = conf['value']
                elif 'mapreduce.workflow' in conf['name']:
                    if 'workflow' not in configs:
                        configs['workflow'] = []
                    configs['workflow'].append({conf['name'] : conf['value']})
            parse_workflow(configs)
            if 'table.name' not in configs:
                configs['table.name'] = 'none'
            return configs
    except IOError as e:
        print(Fore.RED, itera, p, 'is invalid', Style.RESET_ALL)

def get_job_type(path):
    try:
       with open(path+'/conf.json', 'r') as fd:
            data = json.load(fd)
            jtype = ''
            for conf in data['conf']['property']:
                if conf['name'] == 'mapreduce.job.map.class' or conf['name'] == 'mapred.mapper.class':
                    _class = conf['value']
                    if 'hive' in _class:
                       jtype = 'hive'
                    elif 'sqoop' in _class:
                       jtype = 'sqoop'
                    elif 'streaming' in _class:
                       jtype = 'streaming'
                    elif 'distcp' in _class:
                       jtype = 'distributed copy'
                    else:
                       jtype = _class
            return jtype
    except IOError as e:
        print(Fore.RED, itera, p, 'is invalid', Style.RESET_ALL)


def build_io_dependency(conf, io_access, reaccess):
    inputs = conf['inputdir']
    o = conf['outputdir']
    for i in inputs:
        if i not in io_access:
            io_access[i] = {'p' : [], 'c': []}
        if i not in reaccess:
            reaccess[i] = 0
        reaccess[i] += 1
        io_access[i]['c'].append(conf['jobid']) 
    if o not in io_access:
        io_access[o] = {'p' : [], 'c': []}
        reaccess[o] = 0
    io_access[o]['p'].append(conf['jobid'])
    reaccess[o] += 1
    return io_access


def parse_workflow(configs):
    wrkflw = configs['workflow']
    # per job workflow
    adjacency_list = ''
    for wfed in wrkflw:
        for e in wfed:
            if e == 'mapreduce.workflow.name':
                continue
            if e == 'mapreduce.workflow.node.name':
                configs['workflow.node'] = wfed[e]
                cur_node = wfed[e]
            if e.startswith('mapreduce.workflow.adjacency'):
                src = e.replace('mapreduce.workflow.adjacency.', '')
                dst = wfed[e]
                adjacency_list += (',' + src + '>' + dst)
            if e == 'mapreduce.workflow.id':
                 configs['workflow.id'] = wfed[e]
    configs['workflow.dag'] = adjacency_list[1:] if len(adjacency_list) > 0 else cur_node;
    del configs['workflow']
