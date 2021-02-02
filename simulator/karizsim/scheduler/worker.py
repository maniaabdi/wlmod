#!/usr/bin/python3

import simpy

from netsim import PacketGenerator, PacketSink, FlowDemux, SnoopSplitter, WFQServer

class Worker:
    def __init__(self, n_executor):
        self.executors = simpy.Resource(env, capacity=num_executor)

