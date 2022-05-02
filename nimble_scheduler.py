#!/usr/bin/python

from multiprocessing import Pool
import heapq
import functools
import time
import json

#Stores total cost of running all tasks individually
total_cost=0.0
#To compute JCT
job_start_time=sys.float_info.max
job_end_time=0.0

#Stage map stage_id -> stage object
stage_map = {}

#global step dependency model
step_dependency_model = {}

#Priority queue ueue to dispath processes from
scheduled = []

#Used to terminate dispatcher process
no_of_tasks = 0

def exec_task(i,j,exec_file):
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

def interval_overlaps(interval_1,interval_2):
    if interval_1[0] < interval_2[0]:
        if interval_1[1] > interval_2[0]:
            return True
        else:
            return False
    else:
        if interval_2[1] > interval_1[0]:
            return True
        else:
            return False

#Assuming parent started at 0.0
def get_step_finish_time(step):
    parent_stage = stage_map[step['parent'][0:1]]
    step_discrete_intervals = []
    rps = []
    for task in parent_stage['tasks']:
        #Assumption: No two steps of a stage are parents to different steps
        produce_step = task['steps'][len(task['steps'])-1]
        interval = [step_discrete_intervals.append(task['D*']-produce_step['d*']),step_discrete_intervals.append(task['D*'])]
        step_discrete_intervals.append(interval)
        rps.append(produce_step['rp'])
    step_discrete_intervals, rps = (list(t) for t in zip(*sorted(zip(step_discrete_intervals, rps))))
    
    #Compute aggregated rp(t) for each discretized time interval
    step_intervals = []
    new_rps = []
    step_discrete_timestamps = sort(list(chain.from_iterable(step_discrete_intervals)))
    for i in range(1,step_discrete_timestamps):
        step_intervals.append([step_discrete_timestamps[i-1],step_discrete_timestamps[i]])
        new_rps.append(0)
    for i in range(0,len(step_intervals)):
        for j in range(0,len(step_discrete_intervals)):
            if interval_overlaps(step_intervals[i],step_discrete_intervals[j]):
                new_rps[i] += rps[j]

    #Calculate rac for each discretized time interval
    produced_till_here = 0
    consumed_till_here = 0
    actually_consumed_till_here = 0
    time_passed = 0
    racs = []
    for i in range(0,len(step_intervals)):
        time_passed += step_intervals[i][1] - step_intervals[i][0]
        produced_till_here += (step_intervals[i][1] - step_intervals[i][0])*new_rps[i]
        consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*step['rc']
        if produced_till_here < consumed_till_here:
            racs.append(min(new_rps[i],step['rc']))
        else:
            racs.append(step['rc'])
        actually_consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*racs[i]

    #Calculate optimal step finish time (which only considers its parent step)
    te = step_intervals[len(step_intervals)][1]
    if actually_consumed_till_here < produced_till_here:
        te += (produced_till_here - actually_consumed_till_here)/step['rc']
    
    return te

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

def dispatcher(no_of_tasks,start_time):
    start_time = time.time()
    pool = Pool()
    while no_of_tasks > 0:
        if len(scheduled) == 0:
            continue
        i = 0
        while len(scheduled) > 0 and step_dependency_model['stages'][scheduled[i][1]]['tasks'][scheduled[i][2]] >= time.time()-start:
            pool.apply(scheduled[i])


def schedule():
    with open('2_stage_map_reduce.json', 'r') as f:
        step_dependency_model = json.load(f)
    print('Read step dependency model successfully')

    #Nimble scheduler
    current_parents = []
    new_parents = []

    res = []

    #Scheduling initial stages with no parents
    for i in range(0,len(step_dependency_model['stages'])):
        stage = step_dependency_model['stages'][i]
        no_of_tasks += len(stage['tasks'])
        stage_map[stage['stage_id']] = stage
        if stage['parent'] is None:
            new_parents.append(stage['stage_id'])
            for j in range(0,len(stage['tasks'])):
                heapq.heappush(scheduled, (stage['task']['Ts'], functools.partial(exec_task,i,j,stage['exec_file'])))

    #Or fork a new process that polls a queue and schedules tasks as they come
    with Pool() as pool:
        #Find a way to fork and continue on this thread
        res = pool.map_async(smap, scheduled)  
    no_of_tasks -= len(scheduled)
    scheduled = []
    current_parents = new_parents
    new_parents = []

    #Scheduling remaining stages in bfs
    while no_of_tasks > 0:
        for i in range(0,len(step_dependency_model['stages'])):
            stage = step_dependency_model['stages'][i]
            if stage['parent'] in current_parents:
                new_parents.append(stage['stage_id'])
                for j in range(0,len(stage['tasks'])):
                    scheduled.append((functools.partial(exec_task,i,j,stage['exec_file'],i,j))
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