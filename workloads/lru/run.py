#!/usr/bin/python3


import yaml
import sys
from workload import Workload
import simpy


def parse_config(fpath):
    conf = yaml.load(open(fpath, 'r'), Loader=yaml.FullLoader)['workload']
    if conf['interarrival'].lower().endswith('s'):
        conf['interarrival'] = int(conf['interarrival'].lower().rsplit('s', 1)[0])
    
    if conf['duration'].lower().endswith('s'):
        conf['duration'] = int(conf['duration'].lower().rsplit('s', 1)[0])
    elif conf['duration'].lower().endswith('m'):
        conf['duration'] = int(conf['duration'].lower().rsplit('m', 1)[0])*60
    elif conf['duration'].lower().endswith('h'):
        conf['duration'] = int(conf['duration'].lower().rsplit('h', 1)[0])*3600

    if conf['cache']['size'].lower().endswith('g'):
        conf['cache']['size'] = int(conf['cache']['size'].lower().rsplit('g', 1)[0])*(1<<30)

    return conf


if __name__ == '__main__':
    conf_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'
    conf = parse_config(conf_file)
    env = simpy.Environment()
    workload = Workload(conf, type='simulation', env=env)
    env.run(until=conf['duration'])
    workload.dump_stats()
