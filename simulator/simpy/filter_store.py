#!/usr/bin/python3

import simpy
from collections import namedtuple

#FilterStore can, for example, be used to model machine shops where machines have varying attributes. 
# This can be useful if the homogeneous slots of a Resource are not what you need:

Machine = namedtuple('Machine', 'size, duration')
m1 = Machine(1, 2) # Small and slow
m2 = Machine(2, 1) # Big and fast

env = simpy.Environment()
machine_shop = simpy.FilterStore(env, capacity=2)
machine_shop.items = [m1, m2] # Pre-populate the machine shop


def user(name, env, ms, size):
    machine = yield ms.get(lambda machine: machine.size == size)
    print(name, 'got', machine, 'at', env.now)
    yield env.timeout(machine.duration)
    yield ms.put(machine)
    print(name, 'released', machine, 'at', env.now)


users = [env.process(user(i, env, machine_shop, (i % 2) + 1))
        for i in range(3)]

env.run()
