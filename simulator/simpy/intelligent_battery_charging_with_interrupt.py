#!/usr/bin/python3

from random import seed, randint
import simpy

seed(23)

class EV:
    def __init__(self, env):
        self.env = env
        self.drive_proc = env.process(self.drive(env))

    def drive(self, env):
        while True:
            # Drive for 20-40 min
            yield env.timeout(randint(20, 40))

            # Park for 1-6 hours
            print('Start parking at', env.now)
            charging = env.process(self.bat_ctrl(env))
            parking = env.timeout(60)
            yield charging | parking
            if not charging.triggered:
                # Interrupt charging if not already done.
                charging.interrupt('Need to go!')
            print('Stop parking at', env.now)


    def bat_ctrl(self, env):
        while True:
            print('Bat. ctrl. started at', env.now)
            try:
                # Intelligent charging behavior here ...
                yield env.timeout(randint(30, 90))
                print('Bat. ctrl. done at', env.now)
            except simpy.Interrupt as i:
                # Onoes! Got interrupted before the charging was done.
                print('Bat. ctrl. interrupted at', env.now, 'msg:', i.cause)

env = simpy.Environment()
ev = EV(env)
env.run(until=100)

