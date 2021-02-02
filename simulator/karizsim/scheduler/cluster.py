#!/usr/bin/python3


from SparkJob import Job
from netsim import PacketGenerator, PacketSink, SwitchPort, Router, Request
import yaml
import json
import simpy

class Executor(object):
    def __init__(self, env, host, flow_id):
        self.env = env
        self.flow_id = flow_id
        self.out_port = None
        self.in_port = PacketSink(env, debug=False, rec_arrivals=True)
        
        self.task_queue = simpy.Store(env)
        executor = env.process(self.run())
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
        self.out_port.put(Request(time=self.env.now, id=task.input.name, size=task.input.size, reqid= self.request_id, flow_id=self.flow_id))
        #FIXME I should wait for data retrieval event
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
    def __init__(self, env, name, ip, executors, gateway=None):
        self.hostname = name
        self.env = env;
        self.n_exec = executors
        self.executors = [Executor(env, name, i) for i in range(executors)]
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
    def __init__(self, env, topology):
        self.env = env
        self.workers = {}
        self.routers = {}
        self.deploy_cluster(env, topology)

    def deploy_cluster(self, env, fpath):
        try:
            data = yaml.load(open(fpath, 'r'), Loader=yaml.FullLoader)
            topology = data['topology']
            ''' Add nodes '''
            for name in topology:
                node = topology[name]
                name = node['name']
                if node['type'] == 'worker':
                    worker = Worker(env = env, name=name, ip=node['ip'], executors=node['executors'], gateway=node['gateway'])
                    self.workers[name] = worker
                elif node['type'] == 'router':
                    router = Router(env=env, name=name, ip=node['ip'], ports=node['ports'])
                    self.routers[name] = router



                print(name)
        except:
            raise  



    def submit_task(self, wid, task):
        self.workers[wid].submit_task(task)





