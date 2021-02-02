#!/usr/bin/python3


from SparkJob import Job
from SimComponents import PacketGenerator, PacketSink, SwitchPort, Router


class Executor:
    def __init__(self, env, host, wid):
        self.env = env
        self.task_queue = simpy.Store(env, capacity=2)
        executor = env.process(self.run())

        self.id = wid
        self.host = host
        self.pg = PacketGenerator(env, "SJSU", const_arrival, const_size, initial_delay=0.0, finish=35, flow_id=0)
        self.ps = PacketSink(env, debug=False, rec_arrivals=True, selector=selector)
        pass
        

    def submit_task(self, task):
        yield self.task_queue.put(task)
        print(f'Submit task {job} at', self.env.now)
        pass


    def run(self):
        while True:
            yield self.env.timeout(1)
            print('requesting job at', self.env.now)
            task = yield self.store.get()
            print(f' scheduler got {task} at {self.env.now}')
            execute_proc = env.process(self.execute(task))
        pass


    def execute(self, task):
        print(f'Worker {self.wid} reads data for {task.id} at {env.now}')
        # read data
        yield env.timeout(5) # need 5 minutes to fuel the tank

        #process data
        print(f'Worker {self.wid} process data for {task.id} at {env.now}')
        yield env.timeout(5) # need 5 minutes to fuel the tank

        # write data
        print(f'Worker {self.wid} writes data for {task.id} at {env.now}')
        yield env.timeout(5) # need 5 minutes to fuel the tank
        print(f'Car name done refueling at {env.now}')

        # notify the completion of this task
        task.succeed()
        pass


class Worker:
    def __init__(self, env, name, n_executors):
        self.hostname = name
        self.env = env;
        self.n_executors = n_executor;
        self.workers = [Executor(env, hostname, i) for i in range(n_workers)]
        self.cur_exec = 0
        
        self.nic = WFQServer(env, source_rate, [0.5*phi_base, 0.5*phi_base])

        for e in self.workers:
            e.pg.out = self.nic

        pass


    def submit_task(self, task):
        self.workers[self.cur_exec].submit(task)
        self.cur_exec = (self.cur_exec + 1)%self.n_executors
        pass


class Storage:
    def __init__(self, env):
        self.env = env
        self.task_queue = simpy.Store(env, capacity=2)
        executor = env.process(self.run())



class Cluster:
    def __init__(self, env, n_workers, n_executors):
        self.env = env
        self.n_workers = n_workers
        self.workers = [Worker(env, f'w{i}', n_executors) for i in range(self.n_workers)]

        self.switch_port = WFQServer(env, source_rate, [0.5*phi_base, 0.5*phi_base])
        for w in self.workers:
            w.nic.out = self.switch_port


        



    def submit_task(self, wid, task):
        self.workers[wid].submit_task(task)





