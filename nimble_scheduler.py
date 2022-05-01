#!/usr/bin/python

from multiprocessing import Pool
import functools
import time
import json

total_cost=0.0
job_start_time=sys.float_info.max
job_end_time=0.0
stage_map = {}
step_discrete_timestamps = {}

def exec_task(task,exec_file):
    optimal_task_duration = 0.0
    for step in task['steps']:
        steps_launched[step['step_id']] = step
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
        step_discrete_timestamps[step['step_id']] = [start,end]
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

def get_optimal_duration(task):
    return task['D*']

#Assuming parent started at 0.0
def get_step_finish_time(step):
    parent_stage = stage_map[step['parent'][0:1]]
    parent_steps = []
    step_discrete_intervals = []
    rps = []
    for task in parent_stage['tasks']:
        produce_step = task['steps'][len(task['steps'])-1]
        interval = [step_discrete_intervals.append(task['D*']-produce_step['d*']),step_discrete_intervals.append(task['D*'])]
        step_discrete_intervals.append(interval)
        rps.append(produce_step['rp'])
    step_discrete_intervals, rps = (list(t) for t in zip(*sorted(zip(step_discrete_intervals, rps))))

    step_overlapped_intervals = []
    new_rps = []
    start = step_discrete_intervals[0][0]
    end = step_discrete_intervals[0][1]
    current_rp = rps[0]
    for i in range(1,len(step_discrete_intervals)):
        #Fix karo
        if step_discrete_intervals[i][0] < end:
            step_overlapped_intervals.append([start,step_discrete_intervals[i][0]])
            new_rps.append(current_rp)
            step_overlapped_intervals.append([step_discrete_intervals[i][0],min(step_discrete_intervals[i][1],end)])
            new_rps.append(current_rp + rps[i])
            if step_discrete_intervals[i][1] > end:
                current_rp = rps[i]
                start = end
                end = step_discrete_intervals[i][1]
            else:
                start = step_discrete_intervals[i][1]
        else:
            step_overlapped_intervals.append([start,end])
            new_rps.append(current_rp)
            start = step_discrete_intervals[i][0]
            end = step_discrete_intervals[i][1]
            current_rp = rps[i]
    #get rac per interval like rp
    #iterate over intervals and rac and stop at the time when P is consumed

def get_optimal_finish_time(task):
    t_opt = 0.0
    for step in task['steps']:
        if step['has_parent']:
            t = max(get_step_finish_time(task),t_opt + step['d*'])
        else
            t = t_opt + step['d*']
        t_opt = t
    return t_opt    

def get_optimal_launch_time(task):
    D = get_optimal_duration(task)
    Te = get_optimal_finish_time(task)
    return Te - D

def schedule():
    with open('path_to_file/file.json', 'r') as f:
        step_dependency_model = json.load(f)
    print('Read step dependency model successfully')

    #Nimble scheduler
    current_parents = []
    new_parents = []
    scheduled = []

    no_of_tasks = 0

    #Scheduling initial stages with no parents
    for stage in step_dependency_model['stages']:
        no_of_tasks += len(stage['tasks'])
        stage_map[stage['stage_id']] = stage
        if stage['parent'] is None:
            new_parents.append(stage['stage_id'])
            for task in stage['tasks']:
                scheduled.append(functools.partial(exec_task,task)

    #Or fork a new process that polls a queue and schedules tasks as they come
    with Pool() as pool:
        #Find a way to fork and continue on this thread
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
                    get_optimal_launch_time(task)                    
        #Find a way to fork and continue on this thread
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