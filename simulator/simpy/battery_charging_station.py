#!/usr/bin/python3

import simpy

def car(env, name, bcs, driving_time, charge_duration):
    yield env.timeout(driving_time)

    print('%s arriving at %d' % (name, env.now))
    with bcs.request() as req:
        yield req

        print('%s starting to charge at %s' % (name, env.now))
        yield env.timeout(charge_duration)
        print('%s leaving the bcs at %s' % (name, env.now))

env = simpy.Environment()
bcs = simpy.Resource(env, capacity=2)

for i in range(4):
    env.process(car(env, 'Car %d' % i, bcs, i*2, 5))

env.run()
