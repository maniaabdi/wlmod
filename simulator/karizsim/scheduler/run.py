#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler
from cluster import Cluster


env = simpy.Environment()
cluster = Cluster(env, '/local0/wlmod/simulator/karizsim/scheduler/cluster.yaml')
scheduler = Scheduler(env, cluster=cluster)
workload = Workload(env, scheduler)
env.run(until=5)

