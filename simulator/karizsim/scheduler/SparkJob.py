#!/usr/bin/python3
import random


class Task:
    def __init__(self, env, id):
        self.type = 'Map'
        self.inputs = [{'name': , 'size': , 'fetch': env.event()}]
        self.id = id
        self.env = env
        self.completion_event = env.event()


class Stage:
    def __init__(self, env):
        self.env = env
        self.n_tasks = 5# random.randint(1, 10) # FIXME
        self.tasks = [Task(env, t) for t in range(self.n_tasks)]



class Job:
    def __init__(self, env):
        self.env = env
        self.stages = {'0': Stage(env)}
        self.dependencies = {}

    def get_ready_stages(self):
        return self.stages

    def get_ready_tasks(self):
        return self.stages['0'].tasks
