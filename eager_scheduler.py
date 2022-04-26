#!/usr/bin/python

from multiprocessing import Pool
import functools
import time
import json

def exec_task(task):
    step_durations = []
    for step in task['steps']:
        arglist = "("
        arglist += ",".join(arglist)
        arglist += ")"
        start = time.time()
        eval(task['func_name']+arglist)
        end = time.time()
        step_durations.append(end-start)
    step_durations

def smap(f):
    return f()

def schedule():
    with open('path_to_file/file.json', 'r') as f:
        step_dependency_model = json.load(f)

    #Eager
    scheduled = []
    for stage in step_dependency_model['stages']:
        for task in stage['tasks']:
            scheduled.append(functools.partial(exec_task,task)
    
    with Pool() as pool:
        res = pool.map(smap, scheduled)
        print(res)

if __name__ == '__main__':
    schedule()