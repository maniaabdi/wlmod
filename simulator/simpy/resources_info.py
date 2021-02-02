#!/usr/bin/python3

import simpy

def print_stats(res):
    print(f'{res.count} of {res.capacity} slots are allocated.')
    print(f' Users: {res.users}')
    print(f' Queued events: {res.queue}')


def user(res):
    print_stats(res)
    with res.request() as req:
        yield req
        print_stats(res)
    print_stats(res)


env = simpy.Environment()
res = simpy.Resource(env, capacity=1)
procs = [env.process(user(res)), env.process(user(res))]
env.run()
