#!/usr/bin/python3

"""
Use of SimComponents to simulate the network of queues from Homework #6 problem 1, Fall 2014.
See corresponding solution set for mean delay calculation based on Burkes theorem.

Copyright 2014 Dr. Greg M. Bernstein
Released under the MIT license
"""
import random
import functools

import simpy

from SimComponents import PacketGenerator, PacketSink, SwitchPort, Router


if __name__ == '__main__':
    # Set up arrival and packet size distributions
    # Using Python functools to create callable functions for random variates with fixed parameters.
    # each call to these will produce a new random value.
    mean_pkt_size = 100.0  # in bytes
    adist1 = functools.partial(random.expovariate, 2.0)
    adist2 = functools.partial(random.expovariate, 0.5)
    adist3 = functools.partial(random.expovariate, 0.6)
    sdist = functools.partial(random.expovariate, 1.0/mean_pkt_size)
    samp_dist = functools.partial(random.expovariate, 0.50)
    port_rate = 100*8*mean_pkt_size  # want a rate of 2.2 packets per second

    # Create the SimPy environment. This is the thing that runs the simulation.
    env = simpy.Environment()

    # Create the packet generators and sink
    def selector(pkt):
        return pkt.src == "SJSU1"

    def selector2(pkt):
        return pkt.src == "SJSU2"


    def selector3(pkt):
        return pkt.src == "SJSU3"

    ps1 = PacketSink(env, debug=False, rec_arrivals=True, selector=selector)
    ps2 = PacketSink(env, debug=False, rec_waits=True, selector=selector2)
    ps3 = PacketSink(env, debug=False, rec_arrivals=True, selector=selector3)

    pg1 = PacketGenerator(env, "SJSU1", adist1, sdist)
    pg2 = PacketGenerator(env, "SJSU2", adist2, sdist)
    pg3 = PacketGenerator(env, "SJSU3", adist3, sdist)
    router = Router(env, 3)
    switch_port1 = SwitchPort(env, port_rate)
    switch_port2 = SwitchPort(env, port_rate)
    switch_port3 = SwitchPort(env, port_rate)
    switch_port4 = SwitchPort(env, port_rate)

    # Wire packet generators, switch ports, and sinks together
    pg1.out = switch_port1
    pg2.out = switch_port1
    pg3.out = switch_port1
    switch_port1.out = router
    router.outs[0] = switch_port2
    router.outs[1] = switch_port3
    router.outs[2] = switch_port4

    switch_port2.out = ps1
    switch_port3.out = ps2
    switch_port4.out = ps3

    # Run it
    env.run(until=10000)
    print(ps2.waits[-10:])
    # print pm.sizes[-10:]
    # print ps.arrivals[-10:]
    print(f'switch port 1 packet count {switch_port1.packets_rec}, and packet drop {switch_port1.packets_drop}')
    print(f'switch port 2 packet count {switch_port2.packets_rec}, and packet drop {switch_port2.packets_drop}')
    print(f'switch port 3 packet count {switch_port3.packets_rec}, and packet drop {switch_port3.packets_drop}')
    print(f'switch port 4 packet count {switch_port4.packets_rec}, and packet drop {switch_port4.packets_drop}')
    print("average wait source 1 to output 3 = {}".format(sum(ps1.waits)/len(ps1.waits)))
    print("average wait source 2 to output 4 = {}".format(sum(ps2.waits)/len(ps2.waits)))
    print("packets sent {}".format(pg1.packets_sent + pg2.packets_sent + pg3.packets_sent))
    print("packets received: {}".format(len(ps2.waits) + len(ps1.waits) + len(ps3.waits)))
    print(f'packets received: {ps1.packets_rec}, {ps2.packets_rec}, {ps3.packets_rec}')
    # print "average system occupancy: {}".format(float(sum(pm.sizes))/len(pm.sizes))
