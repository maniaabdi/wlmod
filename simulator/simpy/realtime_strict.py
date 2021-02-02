#!/usr/bin/python3

import simpy
import time
import simpy.rt


def slow_proc(env):
    time.sleep(0.02) # Heavy computation :-)
    yield env.timeout(1)

env = simpy.rt.RealtimeEnvironment(factor=0.01, strict=False)
proc = env.process(slow_proc(env))
try:
    env.run(until=proc)
    print('Everything alright')
except RuntimeError:
    print('Simulation is too slow')



env = simpy.rt.RealtimeEnvironment(factor=0.01, strict=True)
proc = env.process(slow_proc(env))
try:
    env.run(until=proc)
    print('Everything alright')
except RuntimeError:
    print('Simulation is too slow')
