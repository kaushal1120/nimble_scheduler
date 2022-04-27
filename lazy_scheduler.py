#!/usr/bin/python

from multiprocessing import Pool
import functools
import time
import json

total_cost=0.0
job_start_time=sys.float_info.max
job_end_time=0.0

def exec_task(task,exec_file):
    for step in task['steps']:
        obj = getattr(__import__(exec_file),exec_file)
        arg_dict = {}
        for i in range(len(step['step_func_arg_keys'])):
            arg_dict[step['step_func_arg_keys'][i]] = step['step_func_arg_vals'][i]
        print('Scheduling task_id: ', task['task_id'], 'of', exec_file, 'at', str(time.time()))
        start = time.time()
        func = getattr(obj, step['step_func_name'])(arg_dict)
        end = time.time()
        total_cost += end-start
        job_end_time = max(job_end_time,end)
        job_start_time = min(job_start_time,start)
    return end

def smap(f):
    return f()

def schedule():
    with open('path_to_file/file.json', 'r') as f:
        step_dependency_model = json.load(f)
    print('Read step dependency model successfully')


    #Lazy scheduler
    current_parents = []
    new_parents = []
    scheduled = []

    no_of_tasks = 0

    #Scheduling initial stages with no parents
    for stage in step_dependency_model['stages']:
        no_of_tasks += len(stage['tasks'])
        if stage['parent'] is None:
            new_parents.append(stage['stage_id'])
            for task in stage['tasks']:
                scheduled.append(functools.partial(exec_task,task)

    with Pool() as pool:
        res = pool.map(smap, scheduled)
        print(res)   
    no_of_tasks -= len(scheduled)
    scheduled = []
    current_parents = new_parents
    new_parents = []

    #Scheduling remaining stages in bfs
    while no_of_tasks > 0:
        for stage in step_dependency_model['stages']:
            if stage['parent'] in current_parents:
                new_parents.append(stage['stage_id'])
                for task in stage['tasks']:
                    scheduled.append(functools.partial(exec_task,task)
        with Pool() as pool:
            res = pool.map(smap, scheduled)
            print(res)
        no_of_tasks -= len(scheduled)
        scheduled = []
        current_parents = new_parents
        new_parents = []
    
    print('Total cost is:', total_cost)
    print('JCT given start time 0.0', job_end_time-job_start_time)

if __name__ == '__main__':
    schedule()