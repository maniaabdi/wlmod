#!/usr/bin/python3

import simpy


def sub(env):
    yield env.timeout(1)
    return 23

def parent(env):
    ret = yield env.process(sub(env))
    print(ret, env.now)
    return ret

env = simpy.Environment()
env.run(env.process(parent(env)))

