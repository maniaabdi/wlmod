#!/usr/bin/python3

import simpy

def my_callback(event):
    print('Call back from', event)


env = simpy.Environment()
event = env.event()
event.callbacks.append(my_callback)
print(event.callbacks)
env.run()
