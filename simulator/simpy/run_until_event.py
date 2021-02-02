#!/usr/bin/python3

import simpy

def my_proc(env):
    yield env.timeout(1)
    return 'Monty Python’s Flying Circus'

env = simpy.Environment()
proc = env.process(my_proc(env))
msg = env.run(until=proc)
print(msg)
