#!/usr/bin/python3


from SparkJob import Job


class Workload:
    def __init__(self, env, scheduler):
        self.env = env
        self.scheduler = scheduler
        generator = env.process(self.job_generator())

    def job_generator(self):
        while True:
            yield self.env.timeout(2)
            job = Job(self.env)
            yield self.scheduler.put(job)
            print(f'Workload generator produced job {job} at {self.env.now}')
