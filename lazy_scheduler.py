#!/usr/bin/python

from multiprocessing import Pool
import functools
import time
import json

total_cost=0.0
job_start_time=sys.float_info.max
job_end_time=0.0

def exec_task(task,exec_file):
    optimal_task_duration = 0.0
    for step in task['steps']:
        obj = getattr(__import__(exec_file),exec_file)

        #Prepare arguments to task exec
        arg_dict = {}
        for i in range(len(step['step_func_arg_keys'])):
            arg_dict[step['step_func_arg_keys'][i]] = step['step_func_arg_vals'][i]
        print('Scheduling task_id: ', task['task_id'], 'of', exec_file, 'at', str(time.time()))

        #Fork new process for task
        start = time.time()
        C,P = getattr(obj, step['step_func_name'])(arg_dict)
        end = time.time()

        #Update P, C, rc, rp, total job cost, optimal step duration, job finish time
        step['P'] = P
        step['C'] = C
        #Averaging for better estimates
        step['rc'] = (step['rc'] + C/(end-start))/(task['no_of_runs']+1)
        step['rp'] = (step['rp'] + P/(end-start))/(task['no_of_runs']+1)
        total_cost += end-start
        optimal_task_duration += end-start
        step['d*'] = (step['d*'] + end - start)/(task['no_of_runs'] + 1)
        job_end_time = max(job_end_time,end)
        job_start_time = min(job_start_time,start)

    #Update optimal task duration, no_of_runs
    task['D*'] = (task['D*'] + end - start)/(task['no_of_runs'] + 1)
    task['no_of_runs'] += 1

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

    # Serializing json and writing to file
    json_object = json.dumps(step_dependency_model, indent = 4)
    with open("sample.json", "w") as outfile:
        outfile.write(json_object)

if __name__ == '__main__':
    schedule()