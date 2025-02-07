#!/usr/bin/python3

import simpy
from simpy.events import AnyOf, AllOf, Event


def fetch_values_of_multiple_events(env):
    t1, t2 = env.timeout(1, value='spam'), env.timeout(2, value='eggs')
    r1, r2 = (yield t1 & t2).values()
    assert r1 == 'spam' and r2 == 'eggs'


def test_condition(env):
    # As a shorthand for AllOf and AnyOf, you can also use the logical operators & (and) and | (or)
    t1, t2 = env.timeout(1, value='spam'), env.timeout(2, value='eggs')
    ret = yield t1 | t2
    assert ret == {t1: 'spam'}

    t1, t2 = env.timeout(1, value='spam'), env.timeout(2, value='eggs')
    ret = yield t1 & t2
    assert ret == {t1: 'spam', t2: 'eggs'}

    e1, e2, e3 = [env.timeout(i) for i in range(3)]
    yield (e1 | e2) & e3
    assert all(e.processed for e in [e1, e2, e3])

env = simpy.Environment()

events = [Event(env) for i in range(3)]
a = AnyOf(env, events) # Triggers if at least one of "events" is triggered.
b = AllOf(env, events)


proc1 = env.process(fetch_values_of_multiple_events(env))
proc2 = env.process(test_condition(env))
env.run()

