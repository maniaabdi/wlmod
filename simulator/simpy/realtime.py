#!/usr/bin/python3

import simpy
import simpy.rt
import time



def example(env):
    start = time.perf_counter()
    yield env.timeout(1)
    end = time.perf_counter()
    print('Duration of one simulation time unit: %.2fs' % (end - start))



env = simpy.Environment()
proc = env.process(example(env))
env.run(until=proc)


# The factor defines how much real time passes with each step of simulation time. 
# By default, this is one second. If you set factor=0.1, a unit of simulation time 
# will only take a tenth of a second; if you set factor=60, it will take a minute.
env = simpy.rt.RealtimeEnvironment(factor=0.1)
proc = env.process(example(env))
env.run(until=proc)
