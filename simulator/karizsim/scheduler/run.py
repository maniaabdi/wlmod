#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler
from cluster import Cluster


env = simpy.Environment()
store = simpy.Store(env, capacity=2)

cluster = Cluster(env, '/local0/wlmod/simulator/karizsim/scheduler/cluster.yaml')
workload = Workload(env, store)
#scheduler = Scheduler(env, store)
env.run(until=5)

