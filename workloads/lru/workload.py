#!/usr/bin/python3
'''
Created on Sep 7, 2019

@author: mania
'''

'''
from alibaba traces we figure it out that average # of DAGs
submitted per 30 is L. The maxumum is Y and the minimum is Z

we generate DAGs for 1 hour every 30 seconds using poisson distribution 
with average time interval of L

For alibaba traces should I randomly 
lets independently apply size and identity 

generate set of N filenames 
Usibg exponential propablity to assign sizes for filenames use zipf distribution 
I use zipf to select from a list of file names

for ali baba traces just randomly assign inputs to nodes. 

'''
import random 
import threading
from colorama import Fore, Style
from threading import Thread
import uuid
import datetime
from prwlock import RWLock
import simpy 
from scipy.stats import expon  
import numpy as np

class CacheElement:
    def __init__(self, name, size):
        self.name = name
        self.size = size
        self.next = None
        self.prev = None


class LruCache:
    def __init__(self, conf):
        print(conf)
        policy = conf['policy']
        self.size = conf['size']
        self.free_size = conf['size']
        self.lru_head = None
        self.lru_tail = None
        self.metadata = {}
        self.hitcount = 0
        self.totalcount = 1



    def lookup_or_insert(self, name, size):
        self.totalcount += 1
        if name in self.metadata:
            self.hitcount += 1
            #print(f'hit {name}')
            return self.touch(self.metadata[name])
        
        if self.free_size < size:
            self.evict(size - self.free_size)
        
        cache_obj = CacheElement(name, size)
        self.touch(cache_obj)
        self.metadata[name] = cache_obj
        self.free_size -= size


    def touch(self, obj: CacheElement):
        if self.lru_head:
            self.lru_head.next = obj

        if obj.next and obj != self.lru_tail:
            obj.prev.next = obj.next
        if obj.prev and obj != self.lru_head:
            obj.next.prev = obj.prev

        obj.prev = self.lru_head
        self.lru_head = obj

        if not self.lru_tail:
            self.lru_tail = obj


    def evict(self, size):
        while self.free_size < size:
            assert(self.lru_tail)
            evict_candidate = self.lru_tail
            self.lru_tail.next.prev = None 
            self.lru_tail = self.lru_tail.next
            self.free_size += evict_candidate.size
            #print(f'evict {evict_candidate.name}')
            del self.metadata[evict_candidate.name]

    def peek(self, name):
        return (name in self.metadata)


class Workload: 
    def __init__(self, conf, type, env=None):
        if conf['type'] == 'cache':
            self.cache = LruCache(conf['cache'])
            self.a= 1.128
            self.workingset_size = 0
            self.map_to_names = {}
            self.debug = False

        assert(type == 'simulation' and env)
        if type == 'simulation':
            self.env = env 
            gen_proc = env.process(self.run_sim(env, conf))


    def run_sim(self, env, conf):
        '''
        the Poisson distribution deals with the number of occurrences in a fixed period of time,
        the Exponential distribution deals with the time between occurrences of successive events as time flows by continuously.
        '''
        ofd = open(conf['output'], 'w')
        while True:
            if self.debug:
                print(f'simtime: {self.env.now}, workingset size: {self.workingset_size}, cache hit ratio: {round(self.cache.hitcount/self.cache.totalcount, 2)}, hit count: {self.cache.hitcount}')
            yield self.env.timeout(expon.rvs(conf['interarrival']))
            rvs = np.random.zipf(self.a, 1)[0]
            name = f'f{len(self.map_to_names)}'
            if rvs not in self.map_to_names:
                self.map_to_names[rvs] = name
            objsize = 4*(1<<20)
            self.cache.lookup_or_insert(name, objsize)
            self.workingset_size = objsize*len(self.map_to_names)
            ofd.write(f'{int(self.env.now)} {name.replace("f","")} {objsize}\n')


    def dump_stats(self):
        print(f'simtime: {self.env.now}, workingset (#objects: {len(self.map_to_names)}, size: {round(self.workingset_size/(1<<30), 2)} GB), cache size: {round(self.cache.size/(1<<30), 2)} GB, cache hit ratio: {round(self.cache.hitcount/self.cache.totalcount, 2)}, hit count: {self.cache.hitcount}, total access: {self.cache.totalcount}')

