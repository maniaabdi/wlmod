#!/usr/bin/python3


from SparkJob import Job
from netsim import PacketGenerator, PacketSink, SwitchPort, Router, Request, NetworkInterface
import yaml
import json
import simpy
import itertools

class Executor(object):
    def __init__(self, env, host, flow_id):
        self.env = env
        self.host = host
        self.flow_id = flow_id
        self.out_port = None
        self.in_port = PacketSink(env, debug=False, rec_arrivals=True)
        
        self.task_queue = simpy.Store(env)
        executor = env.process(self.run())
        pass
        

    def submit(self, task):
        print(f'Submit task {task} to executor {self.flow_id}, worker {self.host} at {self.env.now}')
        yield self.task_queue.put(task)


    def run(self):
        while True:
            yield self.env.timeout(1)
            task = yield self.task_queue.get()
            print(f'Executor {self.flow_id} at worker {self.host} lunches task {task} at {self.env.now}')
            execute_proc = self.env.process(self.execute(task))


    def execute(self, task):
        print(f'Worker {self.flow_id} reads data for {task.id} at {self.env.now}')
        # read data
        read_events = []
        for obj in task.inputs:
            read_evets.append(self.out_port.put(Request(time=self.env.now, id=task.input.name, size=task.input.size, reqid= self.request_id, flow_id=self.flow_id)))    
        yield simpy.events.AllOf(read_events) # need 5 minutes to fuel the tank

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
    def __init__(self, env, name, ip, rate, executors, gateway=None):
        self.hostname = name
        self.env = env;
        self.n_exec = executors
        self.nic = NetworkInterface(env, name=name, ip=ip, rate=rate, flows=executors, gateway=gateway)
        self.executors = {}
        for i in range(executors):
            self.executors[i] = Executor(env, host=name, flow_id=i)
            self.nic.out_ports[i] = self.executors[i].in_port
        self.exec_it = itertools.cycle(self.executors.keys()) 
        pass


    def submit_task(self, task):
        self.env.process(self.executors[next(self.exec_it)].submit(task))


class Storage:
    def __init__(self, env):
        self.env = env
        self.task_queue = simpy.Store(env, capacity=2)
        executor = env.process(self.run())

    def run(self):
        while True:
            task = yield self.task_queue.get()



class Cluster:
    def __init__(self, env, topology):
        self.env = env
        self.workers = {}
        self.routers = {}
        self.storages = {}
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
                    worker = Worker(env = env, name=name, ip=node['ip'], rate=node['rate'], executors=node['executors'], gateway=node['gateway'])
                    self.workers[name] = worker
                elif node['type'] == 'router':
                    router = Router(env=env, name=name, ip=node['ip'], ports=node['ports'], gateway=node['gateway'])
                    self.routers[name] = router
                elif node['type'] == 'storage':
                    storage = Storage(env)
                    self.storages[name] = storage

            ''' connect nodes '''
            for name in self.workers:
                gateway = self.workers[name].nic.gateway
                self.workers[name].nic.out = self.routers[gateway]
                self.routers[gateway].connect(self.workers[name].nic)
            for name in self.routers:
                router = self.routers[name]
                if router.gateway == 'None':
                    continue
                gateway = self.routers[self.routers[name].gateway]
                gateway.connect(router)
                router.connect(gateway, gateway=True)
                gateway.add_route(router.ip)
        except:
            raise  

    def worker_count(self):
        return len(self.workers)

    def get_workers(self):
        return self.workers.keys()

    def submit_task(self, wid, task):
        self.workers[wid].submit_task(task)





