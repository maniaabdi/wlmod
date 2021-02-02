#!/usr/bin/python3

import simpy

def example(env):
    # The Timeout schedules itself at now + delay 
    # (that’s why the environment is required); 
    # other event types usually schedule themselves at the current simulation time.
    event = simpy.events.Timeout(env, delay=1, value=42)

    # The process function then yields the event and thus gets suspended. 
    value = yield event
    print('now=%d, value=%d' % (env.now, value))



env = simpy.Environment()

# these two lines are equivalent to: p = env.process(example(env))
example_gen = example(env)
p = simpy.events.Process(env, example_gen)


env.run()

