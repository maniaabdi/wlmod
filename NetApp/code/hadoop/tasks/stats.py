#!/usr/bin/python
  
import json;


def get_job(path, jobid):
    return Job(path, jobid)

class Job:
    def __init__(self, path, job):
        self.path = path
        self.job = job

    def __str__(self):
        print(self.path)


    def get_job_stats(self):
        with open(self.path+'/' + self.job  +'.json', 'r') as json_file:
            try:
                ResJson= json.load(json_file)
                return {'name': ResJson['job']['name'].encode('utf-8'),
                        'submitTime':  ResJson['job']['submitTime'],
                        'startTime':  ResJson['job']['startTime'],
                        'finishTime': ResJson['job']['finishTime'],
                        'queueTime': ResJson['job']['startTime'] - ResJson['job']['submitTime'],
                        'runTime': ResJson['job']['finishTime'] - ResJson['job']['startTime']}
            except ValueError:
                print('cannot read', self.path+'/' + self.job  +'.json')
