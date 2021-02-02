#!/usr/bin/python3

import simpy

def clock(env, name, tick):
    while True:
        print(name, env.now)
        yield env.timeout(tick)


env = simpy.Environment()
env.process(clock(env, 'fast', 0.5))
env.process(clock(env, 'slow1', 1))
env.process(clock(env, 'slow2', 1))
env.run(until=5)
