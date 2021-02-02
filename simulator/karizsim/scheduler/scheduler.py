#!/usr/bin/python3

import simpy

from netsim import PacketGenerator, PacketSink, FlowDemux, SnoopSplitter, WFQServer

class Scheduler:
    def __init__(self, env, sched_queue, cluster):
        self.env = env
        self.store = sched_queue
        self.cluster = cluster
        env.process(self.schedule())

        self.n_executors = 3
        self.n_workers = 2
        self.workers = [simpy.Resource(env, capacity=self.n_executors) for s in range(self.n_workers)]
        self.prev_worker = 0
        pass

    def schedule(self):
        while True:
            yield self.env.timeout(1)
            print('requesting job at', self.env.now)
            job = yield self.store.get()
            print(f' scheduler got {job} at {self.env.now}')
            execute_proc = env.process(self.admit(job))
        pass
    
    def admit(self, job):
        tasks = job.get_ready_stages()
        for t in tasks:
            w = self.decide_worker()
            self.cluster.add_task(w, t)
            exec_proc = env.process(s.exeucte())
        
        # barrier waits 
        AllOf(env, events)



    def decide_worker(self, env):
        # lets do the round robin for now
        w = (self.prev_worker + 1)%self.n_workers
        self.prev_worker = w
        return w

    def execute(self, execute):
        print(f'Car {self.prev_worker} starts refueling at {env.now}')
        # read data 
        yield env.timeout(5) # need 5 minutes to fuel the tank
        
        #process data
        yield env.timeout(5) # need 5 minutes to fuel the tank

        # write data 
        yield gas_station.gas_tank.get(40)
        print(f'Car name done refueling at {env.now}')

