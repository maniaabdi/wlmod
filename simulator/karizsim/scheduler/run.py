#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler



env = simpy.Environment()
store = simpy.Store(env, capacity=2)
workload = Workload(env, store)
scheduler = Scheduler(env, store)
env.run(until=5)

