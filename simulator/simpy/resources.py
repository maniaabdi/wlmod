#!/usr/bin/python3

import simpy

def resource_user1(env, resource):
    request = resource.request() # Generate a request event
    yield request # Wait for access
    yield env.timeout(1) # Do something
    resource.release(request) # Release the resource

def resource_user(env, resource):
    with resource.request() as req: # Generate a request event
        yield req # Wait for access
        yield env.timeout(1) # Do something
        # Resource released automatically


env = simpy.Environment()
res = simpy.Resource(env, capacity=1)
user = env.process(resource_user(env, res))
env.run()

