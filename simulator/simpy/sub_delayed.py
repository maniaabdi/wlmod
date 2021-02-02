#!/usr/bin/python3

import simpy
from simpy.util import start_delayed


def sub(env):
    yield env.timeout(1)
    return 23

def parent(env):
    # Pay attention to the additional yield needed for the helper process.
    sub_proc = yield start_delayed(env, sub(env), delay=3)
    ret = yield sub_proc
    print(ret, env.now)
    return ret

env = simpy.Environment()
env.run(env.process(parent(env)))

