#!/usr/bin/python3


from SparkJob import Job


class Workload:
    def __init__(self, env, sched_queue):
        self.env = env
        self.store = sched_queue
        generator = env.process(self.job_generator())

    def job_generator(self):
        while True:
            yield self.env.timeout(2)
            job = Job(self.env)
            yield self.store.put(job)
            print(f'Produced job {job} at', self.env.now)
