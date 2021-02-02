#!/usr/bin/python3
import random


class Task:
    def __init__(self, env):
        self.type = 'Map'
        self.env = env

    def execute(self):
        print(f'Car {self.prev_worker} starts refueling at {env.now}')
        # read data
        yield env.timeout(5) # need 5 minutes to fuel the tank

        #process data
        yield env.timeout(5) # need 5 minutes to fuel the tank

        # write data
        yield env.timeout(40)
        print(f'Car name done refueling at {env.now}')




class Stage:
    def __init__(self, env):
        self.env = env
        self.n_tasks = 5# random.randint(1, 10) # FIXME
        self.tasks = [Task(env) for t in range(self.n_tasks)]

    def execute(self, env):
        # choose workers 
        pass 


class Job:
    def __init__(self, env):
        self.env = env
        self.Stage = {'0': Stage(env)}
        self.dependencies = {}
