#!/usr/bin/python

from multiprocessing import Pool
import functools, time, json

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
    with open('2_stage_map_reduce.json', 'r') as f:
        step_dependency_model = json.load(f)
    print('Read step dependency model successfully')

    #Eager scheduler
    scheduled = []
    for stage in step_dependency_model['stages']:
        for task in stage['tasks']:
            scheduled.append(functools.partial(exec_task,task,stage['exec_file']))

    print(schedule)
    
    with Pool() as pool:
        res = pool.map(smap, scheduled)
        print(res)

    print('Total cost is:', total_cost)
    print('JCT given start time 0.0', job_end_time-job_start_time)

if __name__ == '__main__':
    schedule()