#!/usr/bin/python3

'''
Simple example of PacketGenerator and PacketSink from the SimComponents module.
Creates two constant rate packet generators and wires them to one sink.
Note that we can wire as many outputs to an input as we like but to hook a 
single output to multiple inputs requires more work (packet duplication) 
which we have a separate component.

             ___________
S1 -------->|           |
            |  sink     |
S2 -------->|___________|
          

Copyright 2014 Dr. Greg M. Bernstein
Released under the MIT license
'''


from random import expovariate
import simpy
from SimComponents import PacketGenerator, PacketSink


def constArrival():  # Constant arrival distribution for generator 1
    return 1.5

def constArrival2():
    return 2.0

def distSize():
    return expovariate(0.01)

env = simpy.Environment()  # Create the SimPy environment
# Create the packet generators and sink
ps = PacketSink(env, debug=True)  # debugging enable for simple output
pg = PacketGenerator(env, "EE283", constArrival, distSize)
pg2 = PacketGenerator(env, "SJSU", constArrival2, distSize)
# Wire packet generators and sink together
pg.out = ps
pg2.out = ps
env.run(until=20)
